from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.category"))
    .agg(
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
    )
)
