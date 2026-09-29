from pyspark.sql import functions as F

by_dimension = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .groupBy(
        F.col("p.channel").alias("channel"),
        F.col("e.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("e.cost")).alias("min_cost"),
    )
)

result = (
    by_dimension
    .select("channel", "shipment_status", "rows_total", "min_cost")
    .orderBy(F.col("rows_total").desc_nulls_first())
)
