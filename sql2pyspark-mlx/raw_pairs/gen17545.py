from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.sqft") >= 17632, "high").when(F.col("s.sqft") >= 5629, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.min(F.col("i.last_restock")).alias("min_last_restock"),
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
    )
    .filter(F.col("stddev_quantity") > 73)
)
