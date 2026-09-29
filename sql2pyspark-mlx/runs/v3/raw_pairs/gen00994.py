from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .filter(F.year("sale_date") == 2025)
    .groupBy("product_id")
    .agg(F.countDistinct("revenue").alias("unique_stores"))
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("ssbp"), F.col("p.id") == F.col("ssbp.product_id"), "inner")
    .select(F.col("p.id"), F.col("p.product_name"), F.col("ssbp.unique_stores"))
)
