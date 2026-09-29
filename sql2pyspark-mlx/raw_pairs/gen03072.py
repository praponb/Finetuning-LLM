from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["status", "channel"], ["status"], []],
        F.col("p.status").alias("status"),
        F.col("p.channel").alias("channel"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.quantity")).alias("min_quantity"),
        F.grouping_id().alias("level_id"),
    )
)
