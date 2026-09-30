from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.sum("unit_price").alias("total_unit_price"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.avg_unit_price") >= 220)
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("t.avg_unit_price"),
        F.col("t.total_unit_price"),
    )
    .orderBy(F.col("total_unit_price").desc())
)
