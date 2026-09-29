from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("t.unit_price_range"),
        F.col("t.revenue"),
    )
    .orderBy(F.col("cost").desc())
)
