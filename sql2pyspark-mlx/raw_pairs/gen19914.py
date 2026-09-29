from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("a")
    .join(df_orders.alias("dim"), F.col("a.order_id") == F.col("dim.id"), "inner")
    .rollup(F.col("dim.status").alias("status"), F.col("dim.channel").alias("channel"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("a.quantity")).alias("total_quantity"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("total_quantity").desc())
)
