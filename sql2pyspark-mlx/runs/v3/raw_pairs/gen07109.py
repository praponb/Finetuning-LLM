from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter((F.col("s.opened_date") >= "2024-05-01") & (F.col("i.store_id") == 3))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("p.brand"),
        F.col("s.store_name"),
        F.col("s.city"),
        F.trim(F.col("p.brand")).alias("clean_brand"),
    )
)
