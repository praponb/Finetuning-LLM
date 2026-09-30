from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .filter(F.col("units") <= 24)
    .groupBy("product_id")
    .agg(F.min("units").alias("min_units"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .select(F.col("p.brand"), F.col("p.category"), F.col("pt.min_units"))
)
