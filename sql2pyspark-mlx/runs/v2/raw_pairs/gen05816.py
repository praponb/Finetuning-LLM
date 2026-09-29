from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left")
    .select(F.col("p.category"), F.col("p.product_name"), F.col("oi.order_id"))
    .orderBy("product_name", "category")
)
