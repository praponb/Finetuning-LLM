from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("ss.store_id").isin([2, 3]))
    .select(F.col("ss.product_id"), F.col("ss.units"), F.col("p.launch_date"))
)
