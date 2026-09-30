from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promotion_promo_code").orderBy("discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel")
w3 = Window.partitionBy("channel").orderBy(F.col("promo_id").desc_nulls_first(), "id", "promo_id")
w4 = Window.partitionBy("channel").orderBy("promo_id", "id", "promo_id")
w5 = Window.partitionBy("promotion_promo_code").orderBy("ends_on", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

attributed = (
    df_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

enriched = (
    attributed
    .select(
        "*",
        F.first("channel", ignorenulls=True).over(w).alias("first_known_channel"),
        F.round((F.col("promo_id") / F.sum("promo_id").over(w2)), 4).alias("promo_id_share"),
    )
)

filtered = (
    enriched
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_promo_code",
        "first_known_channel",
        F.weekofyear("starts_on").alias("starts_on_weekofyear"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

base = (
    filtered
    .filter(F.col("starts_on") >= F.lit("2023-08-28").cast("date"))
    .select(
        "id",
        "channel",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_promo_code",
        "first_known_channel",
        "channel_clean",
        F.date_sub("starts_on", 73).alias("starts_on_minus_73d"),
    )
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    ranked
    .select(
        "*",
        F.lead("promo_id", 2).over(w4).alias("next_promo_id"),
        F.first("promo_id", ignorenulls=True).over(w5).alias("first_known_promo_id"),
    )
)

daily = (
    metrics
    .groupBy(
        F.date_trunc("week", "ends_on").alias("week_start"),
        F.date_trunc("quarter", "ends_on").alias("quarter_start"),
    )
    .agg(
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.sum("discount_pct").alias("total_discount_pct"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("discount_pct", 0.95).alias("discount_pct_p95"),
        F.countDistinct("promotion_promo_code").alias("distinct_promotion_promo_codes"),
        F.percentile_approx("next_promo_id", [0.25, 0.5, 0.75]).alias("next_promo_id_quartiles"),
    )
    .filter(F.col("distinct_promotion_promo_codes") > 1)
)

result = (
    daily
    .select(
        "week_start",
        "quarter_start",
        "avg_discount_pct",
        "total_discount_pct",
        "distinct_ids",
        "discount_pct_p95",
        "distinct_promotion_promo_codes",
    )
)
