from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("ss.store_id") == 2)
    .select(F.col("ss.id"), F.col("ss.product_id"), F.col("p.product_name"))
    .orderBy("product_name")
)
