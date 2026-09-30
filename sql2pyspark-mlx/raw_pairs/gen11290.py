from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.year("last_restock") == 2024).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_semi")
    .filter(F.col("s.opened_date") > "2024-06-27")
    .select(F.col("s.id"), F.col("s.region"), F.col("s.opened_date"))
    .orderBy(F.col("id").desc())
)
