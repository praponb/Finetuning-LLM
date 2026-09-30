from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "left")
    .select(F.col("p.price"), F.col("p.launch_date"), F.col("oi.product_id"))
)
