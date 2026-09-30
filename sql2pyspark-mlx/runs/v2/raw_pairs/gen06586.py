from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left_anti")
    .filter(F.col("s.sqft") == 12441)
    .select(F.col("s.id"), F.col("s.store_name"))
)
