from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.sum("discount").alias("total_discount"),
    )
    .filter(F.col("cancelled_count") >= 2)
)
