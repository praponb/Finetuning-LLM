from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.region").isin(["East", "Central"]))
    .select(
        F.col("i.quantity"),
        F.col("p.launch_date"),
        F.col("p.product_name"),
        F.col("s.city"),
        F.col("s.region"),
        F.floor(F.col("s.sqft")).alias("sqft_whole"),
    )
    .orderBy("launch_date")
)
