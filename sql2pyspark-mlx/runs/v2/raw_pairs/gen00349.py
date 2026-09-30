from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0)).alias("in_transit_count"),
        F.max("shipped_date").alias("latest_shipped_date"),
    )
    .orderBy(F.col("in_transit_count").desc())
)
