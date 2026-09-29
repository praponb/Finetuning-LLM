from pyspark.sql import functions as F

src = (
    df_inventory
    .filter(F.col("product_id").isin([3, 7]) & F.col("store_id").isin([1, 3]) & (F.col("last_restock") >= "2025-10-01"))
)

inventory_stats = (
    src
    .groupBy("product_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    inventory_stats
    .filter(F.col("stddev_quantity") >= 36)
    .orderBy(F.col("stddev_quantity").desc())
)
