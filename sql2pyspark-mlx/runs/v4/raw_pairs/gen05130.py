from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .filter(F.year("sale_date") == 2025)
    .groupBy("product_id")
    .agg(F.min("units").alias("min_units"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(F.col("p.stock_qty"), F.col("p.supplier_id"), F.col("ps.min_units"))
    .orderBy("min_units")
)
