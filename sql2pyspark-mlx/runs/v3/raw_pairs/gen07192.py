from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter((F.col("i.store_id") == 1) & F.col("i.product_id").isin([3, 8]))
    .select(
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("p.price"),
        F.col("p.launch_date"),
        F.col("s.opened_date"),
    )
)
