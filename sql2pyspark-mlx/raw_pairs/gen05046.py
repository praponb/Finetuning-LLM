from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(F.date_trunc("WEEK", "starts_on").alias("week_start"))
    .agg(
        F.percentile_approx("discount_pct", 0.9).alias("discount_pct_p90"),
        F.min("discount_pct").alias("min_discount_pct"),
        F.percentile_approx("discount_pct", 0.5).alias("discount_pct_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max(F.when(F.col("channel") == "partner", F.col("discount_pct"))).alias("partner_max_discount_pct"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
    )
)
