from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("i.last_restock").between("2023-09-01", "2024-05-09") & (F.col("p.cost") <= 610))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("p.product_name"),
        F.col("p.cost"),
        F.trim(F.col("p.product_name")).alias("clean_product_name"),
    )
)
