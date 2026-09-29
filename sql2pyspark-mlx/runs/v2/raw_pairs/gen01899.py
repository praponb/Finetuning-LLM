from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "ACTIVE", "PAUSED"]) & ((F.col("status") == "EXPIRED") | (F.col("customer_id") == 35)))
    .groupBy("plan")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "basic", 1).otherwise(0))) / F.count("*")), 1).alias("pct_basic"),
        F.countDistinct("monthly_fee").alias("unique_plans"),
    )
)
