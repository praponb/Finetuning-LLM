from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("dim"), F.col("e.order_id") == F.col("dim.id"), "inner")
    .rollup(
        F.col("dim.status").alias("order_status"),
        F.col("e.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("e.cost")).alias("total_cost"),
    )
)
