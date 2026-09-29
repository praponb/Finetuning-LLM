from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(((F.col("delivered_date") < "2025-08-01") & (F.col("shipped_date") < "2024-09-01")) | (F.col("order_id") == 37))
    .groupBy(F.month("shipped_date").alias("shipped_month"), "status", "carrier")
    .agg(
        F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0)).alias("delivered_count"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
    )
    .orderBy(F.col("delivered_count").desc())
)
