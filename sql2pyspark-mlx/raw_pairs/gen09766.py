from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.last_restock") <= "2025-04-03")
    .select(
        F.col("p.launch_date"),
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("i.store_id"),
    )
)
