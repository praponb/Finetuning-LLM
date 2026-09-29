from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .filter(F.col("units").between(14, 33))
    .groupBy("product_id")
    .agg(F.sum("units").alias("total_units"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.total_units") >= 130)
    .select(F.col("p.price"), F.col("ps.total_units"))
    .orderBy(F.col("total_units").desc())
)
