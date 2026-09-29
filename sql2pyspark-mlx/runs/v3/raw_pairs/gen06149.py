from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_count"),
        F.max("shipped_date").alias("last_shipped_date"),
    )
)
