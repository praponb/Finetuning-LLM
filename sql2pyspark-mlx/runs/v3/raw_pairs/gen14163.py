from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.region") != "Central")
    .select(F.col("s.sqft"), F.col("s.city"), F.col("ss.revenue"))
)
