from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.filter(F.col("order_id").isin([17, 38])).alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_semi")
    .filter(F.col("p.price") < 950)
    .select(F.col("p.id"), F.col("p.brand"), F.col("p.cost"))
)
