from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0)).alias("pending_count"),
        F.max("discount").alias("max_discount"),
    )
    .filter(F.col("max_discount") >= 0.21)
    .select("channel", "pending_count", "max_discount")
    .orderBy(F.col("max_discount").desc())
)
