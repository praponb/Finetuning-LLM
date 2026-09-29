from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.month(F.col("i.last_restock")).isin([4, 7, 10]))
    .select(F.col("i.id"), F.col("i.product_id"), F.col("s.opened_date"))
    .orderBy(F.col("id").desc())
)
