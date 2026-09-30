from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(
        F.date_trunc("MONTH", "starts_on").alias("activity_month"),
        F.when(F.col("discount_pct") >= 0.36, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
    )
    .agg(
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("discount_pct_per_mobile"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
    .filter(F.col("distinct_channels") < 1)
)
