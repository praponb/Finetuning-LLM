from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter(F.col("s.store_name").like("%Tow%") | F.col("i.store_id").isin([2, 4, 5]))
    .select(
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("s.sqft"),
        F.col("s.region"),
    )
    .orderBy(F.col("quantity").desc())
)
