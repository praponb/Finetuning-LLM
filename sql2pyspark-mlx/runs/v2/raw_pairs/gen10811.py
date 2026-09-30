from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 3, 4, 7]))
    .groupBy("product_id")
    .agg(F.count("units").alias("units_count"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.units_count") > 4)
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("ps.units_count"))
)
