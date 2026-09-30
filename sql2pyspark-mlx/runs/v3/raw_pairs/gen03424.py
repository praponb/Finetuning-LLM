from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter((F.col("s.city") == "Chiang Mai") & (F.upper(F.trim(F.col("s.store_name"))) == "SIAM FLAGSHIP"))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("s.store_name"),
        F.col("s.sqft"),
    )
)
