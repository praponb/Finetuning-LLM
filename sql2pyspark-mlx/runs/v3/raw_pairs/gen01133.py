from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.filter(F.col("supplier_id").isin([4, 19, 36, 40])).alias("p"), F.col("oi.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("oi.order_id").isin([14, 16, 34]))
    .select(F.col("oi.id"), F.col("oi.unit_price"))
)
