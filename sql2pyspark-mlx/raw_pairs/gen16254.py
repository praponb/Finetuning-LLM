from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("s")
    .join(df_orders.alias("dim"), F.col("s.order_id") == F.col("dim.id"), "left")
    .cube(F.col("dim.channel").alias("channel"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("s.quantity")).alias("max_quantity"),
    )
)
