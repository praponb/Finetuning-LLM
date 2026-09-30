from pyspark.sql import functions as F

by_dimension = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "inner")
    .groupingSets(
        [["channel", "shipment_status"], ["channel"], []],
        F.col("o.channel").alias("channel"),
        F.col("a.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("a.cost")).alias("avg_cost"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    by_dimension
    .select(
        "channel",
        "shipment_status",
        "num_rows",
        "avg_cost",
        "grouping_level",
        F.when(F.col("channel").rlike("^(mobile|web)$"), "GROUP_A").otherwise("GROUP_B").alias("channel_zone"),
    )
    .orderBy(F.col("shipment_status").desc())
)
