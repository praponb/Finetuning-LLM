from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.max("sale_date").alias("latest_sale_date"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("ps.latest_sale_date"))
)
