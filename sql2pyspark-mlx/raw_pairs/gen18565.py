from pyspark.sql import functions as F

dim_totals = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .groupingSets(
        [["order_status", "channel"], ["order_status"], []],
        F.col("d.status").alias("order_status"),
        F.col("d.channel").alias("channel"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("src.cost")).alias("min_cost"),
    )
)

result = (
    dim_totals
    .select(
        "channel",
        "min_cost",
        F.when(F.col("channel").rlike("^(mobile|web)$"), "GROUP_A").when(F.col("channel").rlike("^(store|partner)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("channel_zone"),
    )
    .orderBy(F.col("channel_zone").desc())
)
