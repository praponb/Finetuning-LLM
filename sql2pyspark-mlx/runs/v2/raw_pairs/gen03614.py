from pyspark.sql import functions as F

store_stats = (
    df_orders
    .groupBy("store_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.amount_range") >= 60)
    .select(F.col("s.city"), F.col("s.sqft"), F.col("ss.amount_range"))
    .orderBy(F.col("city").desc())
)
