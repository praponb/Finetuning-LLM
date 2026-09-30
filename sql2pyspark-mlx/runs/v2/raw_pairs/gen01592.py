from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("i.last_restock") < "2023-06-01")
    .groupBy(F.col("p.brand"))
    .agg(
        F.max(F.col("i.quantity")).alias("max_quantity"),
        F.min(F.col("i.quantity")).alias("min_quantity"),
    )
    .orderBy("min_quantity")
)
