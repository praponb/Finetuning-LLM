from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .filter(F.col("o.store_id").isNotNull())
    .join(df_stores.filter(F.col("sqft") != 12455).alias("s"), F.col("o.store_id") == F.col("s.id"), "left_anti")
    .filter(F.col("o.customer_id").isin([10, 11, 24, 32]))
    .select(F.col("o.id"), F.col("o.channel"), F.col("o.status"))
)
