from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("discount_pct").desc())
w2 = Window.partitionBy("channel")
w3 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_rounded").desc_nulls_first())

remaining_keys = (
    df_promotions
    .select("promo_code")
    .subtract(
        df_promotions
        .filter(F.col("ends_on") <= "2024-04-03")
        .select("promo_code")
    )
)

kept_rows = (
    df_promotions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

matched_keys = (
    kept_rows
    .select("id", "promo_code")
    .subtract(
        df_promotions
        .filter(F.col("promo_code").isin(["BUNDLE", "VIP"]))
        .select("id", "promo_code")
    )
)

reconciled = (
    kept_rows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .filter(F.month("starts_on").isin([1, 3, 7]))
    .select("*", F.dense_rank().over(w).alias("discount_pct_dense_rank"))
)

filtered = (
    enriched
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        F.round("discount_pct", 0).alias("discount_pct_rounded"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

metrics = (
    filtered
    .withColumn("discount_pct_dense_rank_share", F.round((F.col("discount_pct_dense_rank") / F.sum("discount_pct_dense_rank").over(w2)), 4))
    .withColumn("discount_pct_rounded_rank", F.rank().over(w3))
)

filtered_promotions = (
    metrics
    .filter(F.col("channel").isNotNull() & (F.year("ends_on") >= 2025))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        "channel_clean",
        "discount_pct_dense_rank_share",
        (F.datediff("starts_on", "ends_on") / 7).alias("weeks_ends_on_to_starts_on"),
    )
)

result = (
    filtered_promotions
    .groupBy(
        "promo_code",
        F.when(F.col("discount_pct_dense_rank_share") >= 0.8, "PREMIUM").otherwise("STANDARD").alias("discount_pct_dense_rank_share_tier"),
    )
    .agg(
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("discount_pct_per_partner"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("discount_pct")).otherwise(0)).alias("mobile_discount_pct"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("discount_pct_per_partner") >= 0)
)
