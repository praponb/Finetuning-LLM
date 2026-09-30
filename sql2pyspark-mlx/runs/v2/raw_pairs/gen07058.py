from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.amount").between(470, 750))
    .select(F.col("s.city"), F.col("s.store_name"), F.col("o.discount"))
    .orderBy("store_name")
)
