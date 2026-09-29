from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-03-05", "2025-06-01"))
    .groupBy("order_id", "status")
    .agg(
        F.min("cost").alias("min_cost"),
        F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0)).alias("delivered_count"),
    )
)
