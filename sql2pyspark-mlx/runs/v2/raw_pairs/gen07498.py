from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
        F.max(F.col("i.quantity")).alias("max_quantity"),
    )
    .filter(F.col("max_quantity") > 70)
    .orderBy("stddev_quantity", "city")
    .limit(5)
)
