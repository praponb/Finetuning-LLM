from pyspark.sql import functions as F

filtered_shipments = df_shipments.filter(F.col("order_id").isNull())

carrier_summary = (
    filtered_shipments
    .groupBy("carrier")
    .agg(
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"),
        F.min("delivered_date").alias("min_delivered_date"),
    )
)

result = (
    carrier_summary
    .filter(F.col("delivered_count") < 9)
    .select("carrier", "delivered_count", "min_delivered_date")
    .orderBy(F.col("delivered_count").desc(), "carrier")
    .limit(10)
)
