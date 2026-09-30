from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("oi.product_id") == 15)
    .select(F.col("p.brand"), F.col("oi.unit_price"))
    .orderBy("unit_price", F.col("brand").desc())
)
