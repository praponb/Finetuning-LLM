from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left_anti")
    .filter(F.col("s.city") != "Chiang Mai")
    .select(F.col("s.id"), F.col("s.opened_date"))
)
