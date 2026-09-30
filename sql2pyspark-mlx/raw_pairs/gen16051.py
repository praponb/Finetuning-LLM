from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_semi")
    .filter(F.col("p.launch_date") < "2024-05-01")
    .select(F.col("p.id"), F.col("p.launch_date"), F.col("p.price"))
)
