from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.filter(F.col("sqft").between(12695, 14656)).alias("s"), F.col("o.store_id") == F.col("s.id"), "left_semi")
    .select(F.col("o.id"), F.col("o.store_id"), F.col("o.channel"))
)
