from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("sqft") >= 8190).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_anti")
    .filter(F.col("i.store_id") == 1)
    .select(F.col("i.id"), F.col("i.quantity"))
)
