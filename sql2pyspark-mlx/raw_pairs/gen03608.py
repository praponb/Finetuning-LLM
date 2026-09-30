from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.id"), F.col("p.brand"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        F.min(F.col("i.last_restock")).alias("first_last_restock"),
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
    )
    .orderBy("id", "brand")
    .limit(3)
)
