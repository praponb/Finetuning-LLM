from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "outer")
    .select(F.col("i.id"), F.col("i.quantity"), F.col("i.product_id"), F.col("s.sqft"))
    .orderBy(F.col("product_id").desc())
)
