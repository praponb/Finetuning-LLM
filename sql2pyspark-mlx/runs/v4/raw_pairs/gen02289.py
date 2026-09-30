from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct_whole", "id")
w2 = Window.partitionBy("promo_code")
w3 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("promo_code").orderBy(F.col("promo_code_count_discount_pct_whole").desc())
w6 = Window.partitionBy("channel", "promo_code")

cleaned = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2023-09-13").cast("date")) & (F.col("starts_on") < "2024-04-01"))
    .select(
        "*",
        F.ceil("discount_pct").alias("discount_pct_whole"),
        F.when(F.col("promo_code").isin(["BUNDLE", "WELCOME5"]), "Tier 1").otherwise("Other").alias("promo_code_group"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.lead("discount_pct", 1).over(w).alias("next_discount_pct"),
        F.min("discount_pct_whole").over(w2).alias("promo_code_min_discount_pct_whole"),
        F.count("discount_pct_whole").over(w2).alias("promo_code_count_discount_pct_whole"),
        F.avg("discount_pct").over(w3).alias("discount_pct_avg_5_prior"),
        F.avg("discount_pct_whole").over(w4).alias("discount_pct_whole_avg_5_prior"),
    )
)

matched = (
    enriched
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

kept_keys = (
    matched
    .select("promo_code")
    .intersectAll(
        df_promotions
        .filter(F.col("promo_code") == "BUNDLE")
        .select("promo_code")
    )
)

reconciled = (
    matched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "*",
        F.rank().over(w5).alias("promo_code_count_discount_pct_whole_rank"),
        F.round((F.col("promo_code_count_discount_pct_whole") / F.sum("promo_code_count_discount_pct_whole").over(w6)), 4).alias("promo_code_count_discount_pct_whole_share"),
    )
)

result = (
    metrics
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        "discount_pct_whole",
        "resolved_channel",
        "promo_code_count_discount_pct_whole",
        "discount_pct_avg_5_prior",
        F.when(F.col("channel").rlike("^(store|web)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("channel_zone"),
    )
    .orderBy(F.col("id").desc(), "ends_on")
)
