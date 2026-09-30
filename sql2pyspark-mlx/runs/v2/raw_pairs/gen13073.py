from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left_anti")
    .select(F.col("s.id"), F.col("s.store_name"), F.col("s.region"), F.col("s.sqft"))
    .orderBy(F.col("id").desc())
)
