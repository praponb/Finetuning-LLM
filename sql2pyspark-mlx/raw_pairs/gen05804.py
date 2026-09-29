from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.amount").isNotNull())
    .select(F.col("o.id"), F.col("o.channel"), F.col("s.region"), F.col("s.city"))
)
