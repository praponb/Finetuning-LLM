from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left_semi")
    .filter(F.col("s.region") == "West")
    .select(F.col("s.id"), F.col("s.sqft"))
    .orderBy(F.col("sqft").desc(), "id")
    .limit(5)
)
