from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("ss.sale_date") < "2023-10-01")
    .groupBy(F.col("p.id"), F.col("p.brand"))
    .agg(F.count(F.col("ss.id")).alias("store_sale_count"))
)
