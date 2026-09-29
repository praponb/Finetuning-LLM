from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.avg_unit_price") >= 73)
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.category"),
        F.col("ps.avg_unit_price"),
        F.col("ps.revenue"),
    )
    .orderBy("cost")
)
