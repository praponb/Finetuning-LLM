from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .select(F.col("oi.unit_price"), F.col("oi.product_id"), F.col("o.store_id"))
    .orderBy(F.col("unit_price").desc(), F.col("product_id").desc())
)
