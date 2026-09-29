from pyspark.sql import functions as F

excluded_stores = df_stores.filter(F.col("city").isin(["Berlin", "Tokyo", "London"])).select("id")

result = (
    df_orders.alias("o")
    .join(excluded_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left_anti")
    .filter(F.col("o.store_id").isNotNull() | F.lit(excluded_stores.isEmpty()))
    .filter(F.col("o.status").isin(["SHIPPED", "RETURNED"]))
    .select(F.col("o.id"), F.col("o.discount"), F.col("o.store_id"), F.col("o.channel"))
    .orderBy("store_id")
)
