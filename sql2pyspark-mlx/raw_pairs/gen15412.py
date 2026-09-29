from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "right")
    .groupBy(F.col("p.category"))
    .agg(
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
        F.countDistinct(F.col("i.quantity")).alias("unique_products"),
    )
    .orderBy(F.col("unique_products").desc())
)
