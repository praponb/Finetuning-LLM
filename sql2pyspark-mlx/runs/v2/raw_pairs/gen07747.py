from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city").isin(["Tokyo", "Berlin", "Chiang Mai"]) & (F.length(F.col("s.store_name")) > 12))
    .select(
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("s.store_name"),
        F.coalesce(F.col("s.sqft"), F.lit(0)).alias("sqft_filled"),
    )
)
