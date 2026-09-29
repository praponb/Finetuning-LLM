from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") > 40) & (F.col("cost").between(29, 51) | (F.col("order_id") == 4)))
    .groupBy("status")
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("shipment_count"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)
