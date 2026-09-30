from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.round(F.avg("units"), 2).alias("avg_units"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.avg_units") > 64)
    .select(F.col("p.id"), F.col("p.cost"), F.col("t.avg_units"))
)
