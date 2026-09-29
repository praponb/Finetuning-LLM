from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "right")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
        F.min(F.col("i.quantity")).alias("min_quantity"),
    )
    .orderBy("stddev_quantity", F.col("region").desc())
)
