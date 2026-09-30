from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("oi.product_id") == 28)
    .groupBy(F.col("p.brand"))
    .agg(F.sum((F.col("oi.unit_price") * F.col("oi.quantity"))).alias("revenue"))
)
