from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.filter(F.col("customer_id") == 6).alias("o"), F.col("s.id") == F.col("o.store_id"), "left_semi")
    .filter(F.col("s.opened_date") >= "2024-06-01")
    .select(F.col("s.id"), F.col("s.opened_date"))
)
