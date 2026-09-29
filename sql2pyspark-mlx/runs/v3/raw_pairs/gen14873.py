from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "right")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter((F.col("s.opened_date") > "2024-07-01") & (F.col("s.sqft") == 7802))
    .select(F.col("i.last_restock"), F.col("s.store_name"), F.col("p.brand"))
)
