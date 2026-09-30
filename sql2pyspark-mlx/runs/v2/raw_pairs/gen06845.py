from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.row_count") > 10)
    .select(F.col("p.price"), F.col("p.supplier_id"), F.col("pt.row_count"))
)
