from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .select(
        F.col("i.quantity"),
        F.col("p.product_name"),
        F.col("s.city"),
        F.col("s.store_name"),
        F.when(F.col("s.sqft") >= 14608, "High").when(F.col("s.sqft") >= 5574, "Medium").otherwise("Low").alias("sqft_band"),
    )
)
