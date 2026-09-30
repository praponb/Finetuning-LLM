from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("i.last_restock").between("2024-01-01", "2024-03-01") & F.col("s.store_name").like("Ma%"))
    .select(
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
        F.col("p.product_name"),
    )
)
