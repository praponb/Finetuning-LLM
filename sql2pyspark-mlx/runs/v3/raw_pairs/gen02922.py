from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.filter(F.col("quantity").between(4, 7)).alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_semi")
    .select(F.col("p.id"), F.col("p.stock_qty"))
    .orderBy("stock_qty", F.col("id").desc())
)
