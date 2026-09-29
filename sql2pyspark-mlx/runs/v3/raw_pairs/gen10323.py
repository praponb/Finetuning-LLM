from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .filter(F.col("product_id").isin([22, 25, 30]))
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.min("unit_price").alias("min_unit_price"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.quantity_range") >= 45)
    .select(
        F.col("p.stock_qty"),
        F.col("ps.quantity_range"),
        F.col("ps.min_unit_price"),
    )
)
