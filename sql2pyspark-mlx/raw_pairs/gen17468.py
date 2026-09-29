from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("i.last_restock") <= "2024-05-01")
    .select(
        F.col("p.price"),
        F.col("p.cost"),
        F.col("i.store_id"),
        F.round(F.col("i.quantity"), 1).alias("quantity_rounded"),
    )
)
