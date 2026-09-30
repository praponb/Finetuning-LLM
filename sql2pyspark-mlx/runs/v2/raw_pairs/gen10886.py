from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.array_contains("tags", "sale")).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("ss.product_id").isin([3, 6, 8, 9]))
    .select(F.col("ss.id"), F.col("ss.store_id"), F.col("ss.units"))
)
