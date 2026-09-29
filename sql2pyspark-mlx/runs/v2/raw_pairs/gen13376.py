from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .filter(F.col("quantity").isNotNull())
    .groupBy("product_id")
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.revenue") > 40)
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("p.supplier_id"),
        F.col("ps.revenue"),
    )
    .orderBy(F.col("id").desc())
)
