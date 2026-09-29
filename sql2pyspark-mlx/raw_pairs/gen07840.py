from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .groupBy(F.month(F.col("p.launch_date")).alias("launch_month"))
    .agg(
        F.min(F.col("oi.unit_price")).alias("min_unit_price"),
        F.countDistinct(F.col("oi.quantity")).alias("unique_products"),
    )
)
