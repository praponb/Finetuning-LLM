from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.col("cost") >= 680).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("ss.revenue").between(1600, 3600))
    .select(F.col("ss.id"), F.col("ss.units"))
    .orderBy("id", "units")
)
