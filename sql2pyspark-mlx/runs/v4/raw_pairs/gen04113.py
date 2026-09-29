from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(F.col("customer_id").isNull()).alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("oi.product_id").isNull())
    .select(F.col("oi.id"), F.col("oi.product_id"))
    .orderBy(F.col("product_id").desc())
)
