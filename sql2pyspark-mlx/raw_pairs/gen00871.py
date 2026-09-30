from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_anti")
    .filter(F.array_contains(F.col("p.tags"), "clearance"))
    .select(F.col("p.id"), F.col("p.price"))
)
