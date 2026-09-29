from pyspark.sql import functions as F

excluded_stores = df_stores.filter(F.col("opened_date") > "2024-07-01").select("id")

result = (
    df_orders.alias("o")
    .join(excluded_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left_anti")
    .filter(F.col("o.store_id").isNotNull() | F.lit(excluded_stores.isEmpty()))
    .filter(F.col("o.customer_id").isin([21, 38]))
    .select(F.col("o.id"), F.col("o.status"), F.col("o.channel"))
    .orderBy(F.col("status").desc(), "channel")
)
