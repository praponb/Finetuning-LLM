from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter((F.col("i.store_id") == 3) & F.col("s.store_name").like("%Kiosk%"))
    .select(
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.when(F.col("s.sqft") >= 12337, "High").when(F.col("s.sqft") >= 3166, "Medium").otherwise("Low").alias("sqft_level"),
    )
)
