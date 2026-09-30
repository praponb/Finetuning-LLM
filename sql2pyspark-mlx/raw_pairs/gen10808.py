from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .rollup(F.col("o.channel").alias("channel"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("e.quantity")).alias("total_quantity"),
        F.round(F.avg(F.col("e.unit_price")), 2).alias("avg_unit_price"),
    )
)
