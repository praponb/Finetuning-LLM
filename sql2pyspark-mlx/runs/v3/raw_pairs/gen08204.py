from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.filter(F.col("order_id").isin([24, 29, 31])).alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_anti")
    .filter(F.col("p.price") <= 830)
    .select(F.col("p.id"), F.col("p.cost"))
    .orderBy("id")
)
