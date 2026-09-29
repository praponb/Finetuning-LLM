from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("quantity").between(100, 202))
    .groupBy("product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .select(F.col("p.launch_date"), F.col("t.min_quantity"), F.col("t.avg_quantity"))
    .orderBy(F.col("avg_quantity").desc())
)
