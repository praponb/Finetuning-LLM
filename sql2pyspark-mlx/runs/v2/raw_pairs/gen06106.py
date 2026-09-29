from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .filter(F.col("sale_date") < "2023-08-11")
    .groupBy("product_id")
    .agg(F.min("sale_date").alias("min_sale_date"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(F.col("p.brand"), F.col("p.cost"), F.col("ps.min_sale_date"))
)
