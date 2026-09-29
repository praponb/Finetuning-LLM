from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.max("sale_date").alias("latest_sale_date"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("P"), F.col("p.id") == F.col("P.product_id"), "inner")
    .select(F.col("p.id"), F.col("p.cost"), F.col("P.latest_sale_date"))
)
