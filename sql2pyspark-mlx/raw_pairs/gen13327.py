from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("units") == 18).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_anti")
    .select(F.col("s.id"), F.col("s.store_name"), F.col("s.region"))
    .orderBy(F.col("id").desc())
    .limit(10)
)
