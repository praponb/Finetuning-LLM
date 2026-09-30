from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("i.store_id").isin([1, 4, 5]) | F.col("s.store_name").like("Sia%"))
    .select(
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("p.brand"),
    )
)
