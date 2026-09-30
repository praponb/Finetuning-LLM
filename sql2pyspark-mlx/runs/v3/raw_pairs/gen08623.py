from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .groupBy(F.col("p.brand"))
    .agg(F.count(F.col("ss.units")).alias("units_count"))
    .filter(F.col("units_count") >= 1)
    .orderBy("brand")
    .limit(10)
)
