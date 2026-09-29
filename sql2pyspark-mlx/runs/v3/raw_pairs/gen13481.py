from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter(F.col("p.stock_qty").between(152, 379) & F.col("p.product_name").like("%Hub%"))
    .select(F.col("p.launch_date"), F.col("oi.product_id"), F.col("oi.order_id"))
    .orderBy(F.col("product_id").desc(), "order_id")
)
