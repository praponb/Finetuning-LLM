from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("store_id").isin([1, 3, 4, 6])).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.region"), F.col("s.sqft"), F.col("s.city"))
    .orderBy("sqft", F.col("id").desc())
)
