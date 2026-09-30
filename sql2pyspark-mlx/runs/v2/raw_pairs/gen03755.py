from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.col("cost") >= 600).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_anti")
    .filter(F.col("ss.product_id").isin([3, 8, 10]))
    .select(F.col("ss.id"), F.col("ss.store_id"))
    .orderBy(F.col("store_id").desc(), "id")
)
