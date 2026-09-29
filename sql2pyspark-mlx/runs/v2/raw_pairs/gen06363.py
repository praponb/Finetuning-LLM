from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .filter(F.col("o.store_id").isNotNull())
    .join(df_stores.filter(F.length("store_name") < 10).alias("s"), F.col("o.store_id") == F.col("s.id"), "left_anti")
    .select(F.col("o.id"), F.col("o.store_id"))
)
