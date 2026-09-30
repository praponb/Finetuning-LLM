from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .filter(F.year(F.col("o.order_date")) == 2024)
    .select(F.col("oi.quantity"), F.col("o.order_date"), F.col("p.brand"))
    .orderBy("order_date")
)
