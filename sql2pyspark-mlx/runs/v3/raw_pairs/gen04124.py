from pyspark.sql import functions as F

excluded_stores = df_stores.filter(F.col("sqft").between(7230, 8580)).select("id")

result = (
    df_orders.alias("o")
    .join(excluded_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left_anti")
    .filter(F.col("o.store_id").isNotNull() | F.lit(excluded_stores.isEmpty()))
    .select(F.col("o.id"), F.col("o.status"), F.col("o.channel"))
    .orderBy("channel")
)
