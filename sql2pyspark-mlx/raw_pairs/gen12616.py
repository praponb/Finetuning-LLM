from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["channel", "shipment_status"], ["channel"], []],
        F.col("d.channel").alias("channel"),
        F.col("e.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("e.cost")).alias("min_cost"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy("shipment_status")
)
