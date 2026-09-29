from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.sqft") < 8431)
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.add_months(F.col("i.last_restock"), 1).alias("last_restock_plus_months"),
    )
    .orderBy("last_restock")
)
