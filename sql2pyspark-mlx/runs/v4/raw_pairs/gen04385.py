from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "inner")
    .select(F.col("s.store_name"), F.col("o.status"))
)
