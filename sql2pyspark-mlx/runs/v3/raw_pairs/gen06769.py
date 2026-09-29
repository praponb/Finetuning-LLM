from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .filter(F.col("store_id").isin([1, 6]))
    .groupBy("store_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("S"), F.col("s.id") == F.col("S.store_id"), "inner")
    .select(F.col("s.id"), F.col("s.sqft"), F.col("S.max_quantity"))
)
