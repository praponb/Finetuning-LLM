from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .groupingSets(
        [["channel", "order_status"], ["channel"]],
        F.col("d.channel").alias("channel"),
        F.col("d.status").alias("order_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("e.cost")).alias("min_cost"),
    )
)
