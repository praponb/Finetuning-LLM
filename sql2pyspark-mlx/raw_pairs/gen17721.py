from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.channel").alias("channel"))
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("e.quantity")).alias("avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc())
)
