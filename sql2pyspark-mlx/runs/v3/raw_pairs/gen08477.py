from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.col("p.category"),
    )
)
