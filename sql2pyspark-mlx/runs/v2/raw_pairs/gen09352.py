from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.filter(F.col("order_date") >= "2023-04-01").alias("o"), F.col("s.id") == F.col("o.store_id"), "left_anti")
    .select(F.col("s.id"), F.col("s.sqft"), F.col("s.region"))
    .orderBy("region", "sqft")
)
