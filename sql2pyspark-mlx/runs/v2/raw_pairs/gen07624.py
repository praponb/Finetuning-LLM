from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.filter(F.upper(F.trim("product_name")) == "LAPTOP PRO 14").alias("p"), F.col("oi.product_id") == F.col("p.id"), "left_semi")
    .select(F.col("oi.id"), F.col("oi.product_id"))
)
