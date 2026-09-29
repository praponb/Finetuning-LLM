from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("oi.order_id").isin([19, 35]))
    .select(F.col("oi.quantity"), F.col("p.launch_date"), F.col("p.cost"))
)
