from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .filter(F.col("store_id") == 2)
    .groupBy("product_id")
    .agg(F.round(F.avg("units"), 2).alias("avg_units"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.avg_units") >= 64)
    .select(F.col("p.id"), F.col("p.cost"), F.col("ps.avg_units"))
    .orderBy("id")
)
