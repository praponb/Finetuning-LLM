from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.col("p.product_name").like("%Shoe%"))
    .select(F.col("p.cost"), F.col("oi.quantity"))
    .orderBy("quantity")
)
