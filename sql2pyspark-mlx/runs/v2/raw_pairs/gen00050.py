from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("store_id").isin([1, 2, 5, 6])).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_anti")
    .filter(F.col("s.region") == "East")
    .select(F.col("s.id"), F.col("s.opened_date"))
    .orderBy(F.col("id").desc(), "opened_date")
)
