from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("P"), F.col("p.id") == F.col("P.product_id"), "inner")
    .filter(F.col("P.max_quantity") > 4.8)
    .select(F.col("p.launch_date"), F.col("P.revenue"), F.col("P.max_quantity"))
    .orderBy("launch_date", F.col("revenue").desc())
)
