from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.col("price") <= 1160).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("ss.store_id").isin([2, 4, 6]))
    .select(F.col("ss.id"), F.col("ss.product_id"))
)
