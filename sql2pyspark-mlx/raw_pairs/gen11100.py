from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("i.product_id").isin([3, 4, 5]))
    .select(
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("p.launch_date"),
    )
)
