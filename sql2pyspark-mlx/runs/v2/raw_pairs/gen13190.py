from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id", "status")
    .agg(F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"))
    .filter(F.col("cancelled_count") < 9)
    .select("customer_id", "status", "cancelled_count")
    .orderBy(F.col("cancelled_count").desc(), "status")
)
