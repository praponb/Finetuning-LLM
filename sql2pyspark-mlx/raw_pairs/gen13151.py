from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "inner")
    .select(
        F.col("s.region"),
        F.col("s.city"),
        F.col("i.last_restock"),
        F.add_months(F.col("i.last_restock"), 3).alias("last_restock_plus_months"),
    )
)
