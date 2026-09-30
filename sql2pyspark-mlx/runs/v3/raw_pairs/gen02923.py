from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .groupBy("customer_id", "status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0))) / F.count("*")), 1).alias("pct_premium"),
        F.min("start_date").alias("first_start_date"),
    )
    .filter(F.col("pct_premium") > 70)
    .select("customer_id", "status", "pct_premium", "first_start_date")
    .orderBy(F.col("customer_id").desc())
)
