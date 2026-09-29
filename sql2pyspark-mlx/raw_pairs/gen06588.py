from pyspark.sql import functions as F

result = (
    df_subscriptions
    .withColumn("monthly_fee_tier", F.when(F.col("monthly_fee") >= 220, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["status", "monthly_fee_tier"], ["status"]],
        "status",
        "monthly_fee_tier",
    )
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee")).otherwise(0)).alias("cancelled_monthly_fee"),
        F.sum(F.when(F.col("plan") == "standard", F.col("monthly_fee")).otherwise(0)).alias("standard_monthly_fee"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("plan") == "standard", 1)), F.lit(0)), 2).alias("monthly_fee_per_standard"),
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
    )
    .orderBy(F.col("monthly_fee_tier").desc_nulls_last())
)
