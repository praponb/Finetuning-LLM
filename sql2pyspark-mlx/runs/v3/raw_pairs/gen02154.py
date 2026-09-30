from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.filter(F.col("launch_date") >= "2025-03-30").alias("p"), F.col("oi.product_id") == F.col("p.id"), "left_semi")
    .select(F.col("oi.id"), F.col("oi.product_id"))
    .orderBy(F.col("id").desc(), "product_id")
)
