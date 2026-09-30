from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .filter(F.col("revenue") <= 3500)
    .groupBy("product_id")
    .agg(F.sum("units").alias("total_units"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.total_units") >= 170)
    .select(F.col("p.price"), F.col("ps.total_units"))
)
