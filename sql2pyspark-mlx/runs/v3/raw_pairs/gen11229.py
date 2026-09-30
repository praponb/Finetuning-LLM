from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("i.product_id") == 2)
    .select(F.col("i.product_id"), F.col("s.city"), F.col("s.region"))
    .orderBy("city")
)
