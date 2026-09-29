from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.count("units").alias("units_count"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.units_count") >= 7)
    .select(F.col("p.supplier_id"), F.col("p.category"), F.col("t.units_count"))
    .orderBy("category")
)
