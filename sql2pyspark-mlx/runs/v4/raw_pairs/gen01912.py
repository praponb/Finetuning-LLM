from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("order_id").isin([2, 3, 10, 13]))
    .groupBy("product_id")
    .agg(F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("p.launch_date"),
        F.col("ps.revenue"),
    )
)
