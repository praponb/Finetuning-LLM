from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "inner")
    .rollup(
        F.col("d.channel").alias("channel"),
        F.col("s.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("s.cost")), 2).alias("total_cost"),
    )
)
