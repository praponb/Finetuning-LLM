from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("dim"), F.col("a.order_id") == F.col("dim.id"), "inner")
    .rollup(
        F.col("dim.channel").alias("channel"),
        F.col("a.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("a.cost")).alias("total_cost"),
    )
)
