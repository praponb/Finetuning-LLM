from pyspark.sql import functions as F

base = (
    df_promotions
    .filter(F.col("starts_on") > F.lit("2023-09-03").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.substring("channel", 1, 2).alias("channel_prefix"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("discount_pct") >= 0.29, "LARGE").otherwise("SMALL").alias("discount_pct_tier"),
    )
    .agg(
        F.percentile_approx("discount_pct", 0.95).alias("discount_pct_p95"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("discount_pct", 0.9).alias("discount_pct_p90"),
    )
    .filter(F.col("distinct_ids") < 1000)
)
