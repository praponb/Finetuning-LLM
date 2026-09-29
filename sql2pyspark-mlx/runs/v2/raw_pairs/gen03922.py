from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.store_id").isin([12, 23, 32, 36]))
    .select(F.col("s.store_name"), F.col("o.channel"), F.col("o.order_date"))
    .orderBy("store_name", F.col("order_date").desc())
)
