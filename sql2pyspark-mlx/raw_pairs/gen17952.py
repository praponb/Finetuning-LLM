from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.date_trunc("WEEK", "start_date").alias("week_start"),
        "plan",
        F.date_trunc("QUARTER", "end_date").alias("quarter_start"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("status") == "CANCELLED", 1)), F.lit(0)), 2).alias("monthly_fee_per_cancelled"),
    )
    .orderBy(F.col("monthly_fee_per_cancelled").desc_nulls_last())
)
