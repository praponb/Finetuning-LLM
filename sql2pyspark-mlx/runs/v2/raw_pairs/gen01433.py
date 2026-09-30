from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(F.countDistinct(F.col("ss.units")).alias("unique_products"))
    .filter(F.col("unique_products") >= 4)
)
