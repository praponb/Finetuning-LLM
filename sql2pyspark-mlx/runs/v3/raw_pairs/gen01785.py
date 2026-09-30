from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("s.sqft").between(5415, 7121))
    .select(
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.col("p.category"),
        F.col("p.launch_date"),
    )
)
