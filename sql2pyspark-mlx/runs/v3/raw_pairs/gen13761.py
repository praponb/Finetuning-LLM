from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.count("*").alias("num_order_items"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("pt.num_order_items"),
        F.col("pt.revenue"),
    )
    .orderBy(F.col("revenue").desc())
)
