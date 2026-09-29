from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .groupBy("product_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.avg_quantity") >= 26)
    .select(F.col("p.id"), F.col("p.launch_date"), F.col("t.avg_quantity"))
    .orderBy("launch_date")
)
