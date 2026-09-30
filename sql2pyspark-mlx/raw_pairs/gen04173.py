from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.region").isNull())
    .select(F.col("ss.store_id"), F.col("s.sqft"), F.col("s.city"), F.col("s.region"))
    .orderBy("sqft")
)
