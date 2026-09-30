from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter(~F.col("p.product_name").like("%Noise%"))
    .select(F.col("oi.product_id"), F.col("oi.order_id"), F.col("p.supplier_id"))
)
