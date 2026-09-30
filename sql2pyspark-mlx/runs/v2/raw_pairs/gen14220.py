from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.sqft").between(12007, 14568))
    .select(F.col("i.id"), F.col("i.quantity"), F.col("s.store_name"))
)
