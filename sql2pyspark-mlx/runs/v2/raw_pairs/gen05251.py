from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter((F.col("s.city") == "London") & F.col("i.product_id").isin([5, 10]))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("s.city"),
        F.col("p.product_name"),
        F.col("p.price"),
        F.upper(F.col("s.store_name")).alias("store_name_upper"),
    )
)
