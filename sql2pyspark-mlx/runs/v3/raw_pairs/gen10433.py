from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city").isin(["New York", "London", "Singapore"]))
    .select(F.col("i.id"), F.col("i.quantity"), F.col("s.city"))
)
