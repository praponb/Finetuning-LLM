from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter(F.col("oi.order_id").isin([1, 11, 20, 34]))
    .select(F.col("p.stock_qty"), F.col("p.category"), F.col("oi.order_id"))
    .orderBy(F.col("category").desc(), "order_id")
)
