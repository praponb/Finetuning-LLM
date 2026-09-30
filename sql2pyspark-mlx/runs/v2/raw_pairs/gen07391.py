from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .filter(F.col("product_id").isin([2, 6, 7, 10]))
    .groupBy("store_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.avg_quantity") >= 140)
    .select(F.col("s.id"), F.col("s.sqft"), F.col("s.city"), F.col("ss.avg_quantity"))
)
