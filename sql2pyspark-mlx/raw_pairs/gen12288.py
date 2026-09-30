from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.round(F.avg("units"), 2).alias("avg_units"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("P"), F.col("p.id") == F.col("P.product_id"), "left")
    .filter(F.col("P.avg_units") >= 45)
    .select(F.col("p.brand"), F.col("P.avg_units"))
    .orderBy(F.col("avg_units").desc())
)
