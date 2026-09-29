from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "outer")
    .filter(~(F.col("p.brand").isin(["Stark", "Globex"]) & (F.col("s.opened_date") <= "2023-04-01")))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("p.category"),
        F.col("s.city"),
    )
)
