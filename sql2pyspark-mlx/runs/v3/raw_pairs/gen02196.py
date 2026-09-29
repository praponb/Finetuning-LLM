from pyspark.sql import functions as F

excluded_stores = df_stores.filter(F.col("store_name").isNotNull()).select("id")

result = (
    df_orders.alias("o")
    .join(excluded_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left_anti")
    .filter(F.col("o.store_id").isNotNull() | F.lit(excluded_stores.isEmpty()))
    .select(F.col("o.id"), F.col("o.customer_id"))
    .orderBy(F.col("id").desc(), F.col("customer_id").desc())
)
