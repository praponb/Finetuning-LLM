from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("store_id").isin([2, 3, 5])).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_anti")
    .filter(F.col("s.city").isin(["Bangkok", "London"]))
    .select(F.col("s.id"), F.col("s.opened_date"))
)
