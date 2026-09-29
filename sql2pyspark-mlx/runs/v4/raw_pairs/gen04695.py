from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .select(F.col("o.amount"), F.col("s.opened_date"), F.col("s.store_name"))
    .orderBy("store_name", "opened_date")
)
