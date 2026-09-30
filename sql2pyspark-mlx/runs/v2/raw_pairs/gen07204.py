from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("i.store_id").isin([1, 2, 4, 5]))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("s.region"),
        F.col("p.launch_date"),
        F.col("p.price"),
        F.add_months(F.col("i.last_restock"), 3).alias("last_restock_plus_months"),
    )
    .orderBy("last_restock_plus_months")
)
