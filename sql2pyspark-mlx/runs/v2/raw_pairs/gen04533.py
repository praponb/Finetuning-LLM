from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .filter(F.col("s.city").isin(["Chiang Mai", "Singapore"]) & (F.col("p.category") == "Fashion"))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("s.sqft"),
        F.col("p.product_name"),
    )
)
