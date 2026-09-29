from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.id"), F.col("p.product_name"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
    )
    .orderBy(F.col("stddev_quantity").desc(), "id", "product_name")
    .limit(5)
)
