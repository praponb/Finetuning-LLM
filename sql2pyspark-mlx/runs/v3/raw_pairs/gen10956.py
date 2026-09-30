from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_count"))
    .filter(F.col("expired_count") < 2)
    .orderBy(F.col("customer_id").desc(), F.col("expired_count").desc())
)
