from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(~F.col("s.store_name").like("Ma%"))
    .select(F.col("o.store_id"), F.col("s.region"), F.col("s.store_name"))
)
