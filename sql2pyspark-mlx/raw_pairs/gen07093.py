from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("i.product_id").isin([1, 5, 6, 9]))
    .groupBy(F.col("s.city"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
    )
    .filter(F.col("stddev_quantity") >= 33)
    .orderBy(F.col("inventory_count").desc())
)
