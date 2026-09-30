from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.count("*").alias("total_count"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.total_count") >= 4)
    .select(F.col("p.id"), F.col("p.cost"), F.col("pt.total_count"))
)
