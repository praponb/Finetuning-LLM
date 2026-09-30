from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.sqft").between(8713, 13123) | (F.col("ss.store_id") == 3))
    .select(F.col("s.store_name"), F.col("ss.product_id"), F.col("ss.store_id"))
    .orderBy("store_name")
)
