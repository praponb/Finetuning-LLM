from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("p.product_name").like("%Mat%"))
    .groupBy(F.col("p.category"))
    .agg(
        F.max(F.col("i.last_restock")).alias("last_last_restock"),
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
    )
    .orderBy(F.col("category").desc())
)
