from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "outer")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "outer")
    .filter(F.col("i.store_id").isin([2, 5, 6]) & F.col("p.supplier_id").isin([11, 25]))
    .select(
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("p.launch_date"),
        F.col("s.sqft"),
        F.col("s.store_name"),
    )
)
