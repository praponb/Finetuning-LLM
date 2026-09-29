from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_semi")
    .filter(F.col("p.price") >= 1190)
    .select(F.col("p.id"), F.col("p.product_name"))
)
