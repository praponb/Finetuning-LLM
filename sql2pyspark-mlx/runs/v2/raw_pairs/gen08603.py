from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(F.countDistinct("amount").alias("unique_stores"))
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("O"), F.col("s.id") == F.col("O.store_id"), "left")
    .select(F.col("s.id"), F.col("s.city"), F.col("s.sqft"), F.col("O.unique_stores"))
    .orderBy(F.col("id").desc())
)
