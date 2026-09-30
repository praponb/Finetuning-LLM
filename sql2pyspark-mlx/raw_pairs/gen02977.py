from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .groupBy(
        F.when(F.col("s.sqft") >= 13180, "high").when(F.col("s.sqft") >= 3486, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"))
    .orderBy(F.col("stddev_quantity").desc(), "sqft_bucket")
)
