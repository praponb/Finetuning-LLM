from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("store_id").isin([2, 5])).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_anti")
    .filter(F.col("s.sqft").isNull())
    .select(F.col("s.id"), F.col("s.city"), F.col("s.region"))
    .orderBy("region")
)
