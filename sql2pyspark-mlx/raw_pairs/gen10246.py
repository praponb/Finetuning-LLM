from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "eco"))
    .groupBy(F.col("s.city"))
    .agg(F.countDistinct(F.col("ss.units")).alias("unique_stores"))
    .orderBy(F.col("unique_stores").desc())
)
