from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.month(F.col("s.opened_date")).isin([2, 6, 10]) | F.month(F.col("p.launch_date")).isin([6, 7, 8]))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("p.price"),
        F.col("p.cost"),
        F.col("s.region"),
        F.col("s.sqft"),
    )
)
