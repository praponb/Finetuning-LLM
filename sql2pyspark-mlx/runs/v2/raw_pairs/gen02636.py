from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.filter(F.col("amount") > 890).alias("o"), F.col("s.id") == F.col("o.store_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.sqft"), F.col("s.store_name"), F.col("s.region"))
)
