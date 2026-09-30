from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") >= "2024-12-28")
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", 1).otherwise(0)).alias("enterprise_count"),
    )
    .orderBy(F.col("customer_id").desc(), "enterprise_count")
    .limit(50)
)
