from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("quantity").between(5, 8))
    .groupBy("product_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.max_quantity") >= 7.0)
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("ps.avg_unit_price"),
        F.col("ps.max_quantity"),
    )
    .orderBy(F.col("max_quantity").desc())
)
