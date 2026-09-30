from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter((F.col("ss.revenue") > 3000) & F.col("ss.store_id").isin([2, 5]))
    .select(F.col("ss.id"), F.col("ss.product_id"), F.col("p.product_name"))
)
