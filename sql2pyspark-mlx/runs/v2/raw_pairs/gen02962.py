from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") < "2023-09-01")
    .groupBy("order_id")
    .agg(
        F.max("shipped_date").alias("latest_shipped_date"),
        F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_count"),
    )
    .orderBy("order_id")
)
