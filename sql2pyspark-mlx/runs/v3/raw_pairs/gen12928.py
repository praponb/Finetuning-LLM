from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("s.store_name"),
        F.col("s.sqft"),
    )
    .orderBy("quantity")
)
