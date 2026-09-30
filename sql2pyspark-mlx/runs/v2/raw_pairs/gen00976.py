from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("i.store_id") == F.col("s.id"), "left")
    .select(
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.when(F.col("s.region") == "East", 1).otherwise(0).alias("is_east"),
    )
    .orderBy("city")
)
