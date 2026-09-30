from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "right")
    .filter(F.col("i.quantity") != 133)
    .select(
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("p.cost"),
        F.col("p.brand"),
        F.col("s.store_name"),
    )
)
