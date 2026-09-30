from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.filter(F.month("launch_date").isin([3, 8, 12])).alias("p"), F.col("oi.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("oi.product_id").isNotNull())
    .select(F.col("oi.id"), F.col("oi.product_id"), F.col("oi.quantity"))
)
