from pyspark.sql import functions as F

result = (
    df_subscriptions
    .withColumn("quarter_start", F.date_trunc("QUARTER", "end_date"))
    .cube("quarter_start")
    .agg(
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("plan") == "premium", 1)), F.lit(0)), 2).alias("monthly_fee_per_premium"),
        F.percentile_approx("monthly_fee", 0.9).alias("monthly_fee_p90"),
        F.grouping("quarter_start").alias("gid"),
    )
    .orderBy(F.col("monthly_fee_per_premium").desc_nulls_first())
)
