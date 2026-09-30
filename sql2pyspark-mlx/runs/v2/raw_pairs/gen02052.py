from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"),
        F.max("shipped_date").alias("latest_shipped_date"),
    )
)
