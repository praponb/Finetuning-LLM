from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("product_id").isin([2, 4, 6, 7]))
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(F.col("p.price"), F.col("t.total_quantity"))
    .orderBy(F.col("price").desc())
)
