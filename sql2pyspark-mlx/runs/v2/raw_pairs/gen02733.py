from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .select(
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("s.city"),
        F.col("p.supplier_id"),
        F.col("p.product_name"),
        F.concat(F.col("s.store_name"), F.lit(" - "), F.col("s.region")).alias("store_name_region"),
    )
)
