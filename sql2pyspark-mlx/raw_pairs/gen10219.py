from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("start_date") <= "2024-02-08") & F.col("customer_id").isNotNull() & (F.col("status") == "PAUSED"))
    .groupBy("status", F.year("start_date").alias("start_year"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "enterprise", 1).otherwise(0))) / F.count("*")), 1).alias("pct_enterprise"),
        F.min("start_date").alias("first_start_date"),
    )
)
