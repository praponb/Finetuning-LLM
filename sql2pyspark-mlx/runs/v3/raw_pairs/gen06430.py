from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("city").isin(["New York", "Bangkok", "Berlin"])).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_semi")
    .filter(F.month(F.col("i.last_restock")).isin([2, 3, 10]))
    .select(F.col("i.id"), F.col("i.product_id"))
    .orderBy(F.col("id").desc())
)
