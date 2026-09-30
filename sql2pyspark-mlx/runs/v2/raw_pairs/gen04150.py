from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.month("last_restock").isin([1, 4, 11])).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.opened_date"), F.col("s.sqft"), F.col("s.region"))
)
