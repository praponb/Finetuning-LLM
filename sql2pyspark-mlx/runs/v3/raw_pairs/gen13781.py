from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("p.brand"))
    .agg(F.count(F.col("ss.units")).alias("units_count"))
    .orderBy(F.col("brand").desc())
)
