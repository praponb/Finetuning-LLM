from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.max("sale_date").alias("latest_sale_date"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .select(F.col("p.category"), F.col("pt.latest_sale_date"))
    .orderBy("category")
)
