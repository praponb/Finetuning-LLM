from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("product_id").isin([2, 7])).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_semi")
    .filter(F.col("s.city") == "New York")
    .select(F.col("s.id"), F.col("s.sqft"), F.col("s.store_name"), F.col("s.region"))
)
