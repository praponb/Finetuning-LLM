from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("s.sqft").between(6596, 8521))
    .groupBy(
        F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"),
        F.when(F.col("s.sqft") >= 16806, "high").when(F.col("s.sqft") >= 2527, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
        F.max(F.col("i.quantity")).alias("max_quantity"),
    )
)
