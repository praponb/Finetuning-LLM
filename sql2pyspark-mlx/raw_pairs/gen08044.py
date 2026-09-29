from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("quantity").between(168, 187)).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_anti")
    .select(F.col("s.id"), F.col("s.city"))
    .orderBy(F.col("id").desc())
)
