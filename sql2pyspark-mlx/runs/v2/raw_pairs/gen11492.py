from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.when(F.col("s.sqft") >= 12906, "High").when(F.col("s.sqft") >= 7370, "Medium").otherwise("Low").alias("sqft_band"),
    )
    .orderBy(F.col("store_id").desc())
)
