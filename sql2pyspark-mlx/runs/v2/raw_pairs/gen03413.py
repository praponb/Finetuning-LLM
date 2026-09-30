from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("p.launch_date"),
        F.col("p.product_name"),
        F.add_months(F.col("i.last_restock"), 1).alias("last_restock_plus_months"),
    )
)
