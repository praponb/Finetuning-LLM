from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("quantity") < 85).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.region"), F.col("s.sqft"), F.col("s.city"))
    .orderBy("id")
    .limit(10)
)
