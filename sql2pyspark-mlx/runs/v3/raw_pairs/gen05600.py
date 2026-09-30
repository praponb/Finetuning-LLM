from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("i.store_id").isin([1, 2, 3, 5]) & ~F.col("s.city").isin(["New York", "Tokyo", "Berlin"]))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("s.region"),
        F.col("s.city"),
        F.col("s.sqft"),
    )
)
