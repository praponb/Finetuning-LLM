from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(F.year("order_date") == 2023).alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("oi.order_id").isin([8, 11, 21]))
    .select(F.col("oi.id"), F.col("oi.quantity"))
)
