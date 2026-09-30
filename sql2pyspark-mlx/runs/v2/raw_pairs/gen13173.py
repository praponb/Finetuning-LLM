from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .filter(F.col("revenue") > 1100)
    .groupBy("product_id")
    .agg(F.avg("units").alias("avg_units"))
)

result = (
    df_products.alias("products")
    .join(product_summary.alias("t"), F.col("products.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.avg_units") >= 27)
    .select(F.col("products.id"), F.col("products.launch_date"), F.col("t.avg_units"))
    .orderBy(F.col("launch_date").desc(), "id")
    .limit(5)
)
