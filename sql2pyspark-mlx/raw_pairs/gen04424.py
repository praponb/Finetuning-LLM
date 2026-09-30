from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("a")
    .join(df_orders.alias("dim"), F.col("a.order_id") == F.col("dim.id"), "left")
    .rollup(F.col("dim.channel").alias("channel"), F.col("dim.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("a.quantity")).alias("total_quantity"),
        F.avg(F.col("a.unit_price")).alias("avg_unit_price"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy("channel")
)
