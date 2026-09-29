from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .groupBy(F.year(F.col("p.launch_date")).alias("launch_year"))
    .agg(F.max(F.col("ss.sale_date")).alias("latest_sale_date"))
    .orderBy(F.col("launch_year").desc())
)
