from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.customer_id").isNotNull() & (F.col("s.opened_date") < "2023-10-01"))
    .select(F.col("o.id"), F.col("o.amount"), F.col("s.region"), F.col("s.store_name"))
    .orderBy(F.col("store_name").desc(), "id")
    .limit(20)
)
