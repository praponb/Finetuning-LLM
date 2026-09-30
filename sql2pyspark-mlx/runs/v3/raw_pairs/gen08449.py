from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.col("sqft") != 6911).alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("ss.store_id").isin([2, 3, 4, 6]))
    .select(F.col("ss.id"), F.col("ss.store_id"))
)
