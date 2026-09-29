from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.filter(F.col("unit_price") > 110).alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left_semi")
    .select(F.col("o.id"), F.col("o.amount"))
    .orderBy(F.col("amount").desc(), "id")
    .limit(10)
)
