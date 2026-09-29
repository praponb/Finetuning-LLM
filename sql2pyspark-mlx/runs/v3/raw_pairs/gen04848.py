from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("o.shipping_fee") <= 31)
    .select(F.col("s.region"), F.col("s.store_name"), F.col("o.store_id"))
)
