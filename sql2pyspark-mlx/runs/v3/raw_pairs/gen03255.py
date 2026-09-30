from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "premium")
    .groupBy(
        F.when(F.year("start_date") < 2024, "before_2024").when(F.year("start_date") == 2024, "2024").otherwise("after_2024").alias("start_cohort"),
        "plan",
    )
    .agg(
        F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0)).alias("expired_count"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"),
    )
    .filter(F.col("active_count") >= 6)
    .select("start_cohort", "plan", "expired_count", "active_count")
)
