from pyspark.sql import functions as F

store_totals = (
    df_orders
    .filter(F.col("store_id").isin([6, 31]))
    .groupBy("store_id")
    .agg(F.min("order_date").alias("min_order_date"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(F.col("s.sqft"), F.col("s.region"), F.col("t.min_order_date"))
    .orderBy(F.col("sqft").desc())
)
