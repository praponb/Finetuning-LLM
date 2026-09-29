from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.city"))
    .agg(F.max(F.col("i.quantity")).alias("max_quantity"))
    .filter(F.col("max_quantity") > 200)
    .orderBy(F.col("max_quantity").desc(), "city")
    .limit(50)
)
