from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.filter(F.col("price") <= 570).alias("p"), F.col("oi.product_id") == F.col("p.id"), "left_semi")
    .select(F.col("oi.id"), F.col("oi.unit_price"), F.col("oi.quantity"))
    .orderBy("unit_price", "id")
    .limit(10)
)
