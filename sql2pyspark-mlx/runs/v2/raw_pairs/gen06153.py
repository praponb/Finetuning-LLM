from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .filter(F.col("o.store_id").isNotNull())
    .join(df_stores.filter(F.col("sqft") < 5978).alias("s"), F.col("o.store_id") == F.col("s.id"), "left_anti")
    .filter(F.col("o.discount") <= 0.11)
    .select(F.col("o.id"), F.col("o.shipping_fee"))
)
