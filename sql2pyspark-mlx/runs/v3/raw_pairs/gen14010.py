from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.filter(F.col("shipping_fee") < 36).alias("o"), F.col("s.id") == F.col("o.store_id"), "left_anti")
    .filter(F.col("s.region") == "East")
    .select(F.col("s.id"), F.col("s.sqft"))
    .orderBy(F.col("sqft").desc())
)
