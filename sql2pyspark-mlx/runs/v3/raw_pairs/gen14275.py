from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .filter(F.year("sale_date") == 2024)
    .groupBy("product_id")
    .agg(F.min("units").alias("min_units"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("P"), F.col("p.id") == F.col("P.product_id"), "inner")
    .filter(F.col("P.min_units") >= 38)
    .select(F.col("p.product_name"), F.col("p.launch_date"), F.col("P.min_units"))
)
