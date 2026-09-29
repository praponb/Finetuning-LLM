from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("product_id") == 6).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_semi")
    .filter(F.col("s.sqft") == 4726)
    .select(F.col("s.id"), F.col("s.opened_date"), F.col("s.region"))
    .orderBy("id", F.col("region").desc())
)
