from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter((F.year(F.col("s.opened_date")) == 2024) & F.col("i.store_id").isin([1, 2, 3, 4]))
    .select(F.col("i.id"), F.col("i.last_restock"), F.col("p.price"), F.col("s.sqft"))
)
