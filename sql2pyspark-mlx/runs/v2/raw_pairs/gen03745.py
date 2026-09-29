from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("product_id").isin([3, 4, 5]))
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.avg_quantity") > 180)
    .select(F.col("p.id"), F.col("p.product_name"), F.col("t.avg_quantity"))
)
