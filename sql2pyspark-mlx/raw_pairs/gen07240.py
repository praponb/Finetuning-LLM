from pyspark.sql import functions as F

rollup_totals = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["channel", "order_status"], ["channel"]],
        F.col("p.channel").alias("channel"),
        F.col("p.status").alias("order_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("e.cost")).alias("max_cost"),
    )
)

result = (
    rollup_totals
    .select(
        "channel",
        "rows_total",
        "max_cost",
        F.when(F.col("channel").rlike("^(partner|web)$"), "GROUP_B").when(F.col("channel").rlike("^(mobile|store)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("channel_zone"),
    )
    .orderBy(F.col("max_cost").asc_nulls_first(), "channel_zone")
)
