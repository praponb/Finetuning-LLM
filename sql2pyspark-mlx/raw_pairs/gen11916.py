from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .select(
        F.col("i.store_id"),
        F.col("p.price"),
        F.col("s.store_name"),
        F.round(F.col("s.sqft"), 2).alias("sqft_rounded"),
    )
)
