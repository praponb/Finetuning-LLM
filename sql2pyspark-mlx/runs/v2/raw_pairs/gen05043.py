from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.filter(F.length("product_name") < 7).alias("p"), F.col("oi.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("oi.product_id").isin([20, 27, 40]))
    .select(F.col("oi.id"), F.col("oi.quantity"))
    .orderBy(F.col("id").desc())
    .limit(10)
)
