from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("store_id") == 3).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_semi")
    .filter(F.month(F.col("s.opened_date")).isin([5, 10, 12]))
    .select(F.col("s.id"), F.col("s.region"), F.col("s.store_name"), F.col("s.sqft"))
    .orderBy("store_name", "region", "id")
    .limit(5)
)
