from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2023-03-01", "2023-07-09"))
    .groupBy("status", F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0)).alias("pending_count"),
    )
    .filter(F.col("pending_count") >= 49)
    .select("status", "order_quarter", "pending_count")
)
