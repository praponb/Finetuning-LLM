from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("ss.store_id").isin([2, 3, 4, 5]))
    .select(F.col("ss.id"), F.col("ss.units"), F.col("p.category"), F.col("p.brand"))
)
