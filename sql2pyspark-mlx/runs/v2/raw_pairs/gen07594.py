from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.region") == "West")
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("s.store_name"),
        F.add_months(F.col("i.last_restock"), 1).alias("last_restock_plus_months"),
    )
)
