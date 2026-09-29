from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter((F.col("s.city") == "Singapore") & F.col("s.store_name").like("O%"))
    .select(F.col("i.quantity"), F.col("i.last_restock"), F.col("s.city"))
)
