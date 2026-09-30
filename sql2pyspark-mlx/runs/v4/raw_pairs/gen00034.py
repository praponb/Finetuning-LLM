from pyspark.sql import functions as F

result = (
    df_promotions
    .withColumn("activity_month", F.date_trunc("MONTH", "ends_on"))
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.35, "PREMIUM").otherwise("STANDARD"))
    .rollup("activity_month", "discount_pct_tier")
    .agg(
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("activity_month").desc())
)
