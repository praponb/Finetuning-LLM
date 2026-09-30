from pyspark.sql import functions as F

rollup_totals = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("dim"), F.col("e.order_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["channel", "order_status", "shipment_status"], ["channel", "order_status"], ["channel"]],
        F.col("dim.channel").alias("channel"),
        F.col("dim.status").alias("order_status"),
        F.col("e.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("e.cost")).alias("min_cost"),
    )
)

result = (
    rollup_totals
    .select(
        "order_status",
        "cnt",
        "min_cost",
        F.when(F.col("order_status").rlike("^(COMPLETED|SHIPPED)$"), "GROUP_B").when(F.col("order_status").rlike("^(PENDING|CANCELLED)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("order_status_zone"),
    )
    .orderBy(F.col("cnt").asc_nulls_first())
)
