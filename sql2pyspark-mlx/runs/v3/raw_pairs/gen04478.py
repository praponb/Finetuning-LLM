from pyspark.sql import functions as F

excluded_stores = df_stores.filter(F.col("sqft") != 8917).select("id")

result = (
    df_orders.alias("o")
    .join(excluded_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left_anti")
    .filter(F.col("o.store_id").isNotNull() | F.lit(excluded_stores.isEmpty()))
    .filter(F.col("o.customer_id").isNotNull())
    .select(F.col("o.id"), F.col("o.channel"))
)
