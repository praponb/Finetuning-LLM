from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.store_id").isin([1, 3, 5]) | F.array_contains(F.col("p.tags"), "clearance"))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("s.sqft"),
        F.col("p.price"),
        F.col("p.brand"),
        F.when(F.col("i.quantity") >= 181, "High").when(F.col("i.quantity") >= 110, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)
