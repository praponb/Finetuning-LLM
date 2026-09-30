from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("quantity") > 205)
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("P"), F.col("p.id") == F.col("P.product_id"), "inner")
    .select(F.col("p.category"), F.col("P.avg_quantity"))
)
