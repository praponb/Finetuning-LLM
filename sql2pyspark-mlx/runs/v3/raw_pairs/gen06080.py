from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.filter(F.year("launch_date") == 2023).alias("p"), F.col("oi.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("oi.order_id") == 22)
    .select(F.col("oi.id"), F.col("oi.product_id"))
)
