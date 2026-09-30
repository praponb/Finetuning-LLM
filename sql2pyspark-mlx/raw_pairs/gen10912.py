from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter((F.col("i.quantity") > 236) & (F.size(F.col("p.tags")) > 1))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("p.cost"),
        F.col("p.brand"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
    )
)
