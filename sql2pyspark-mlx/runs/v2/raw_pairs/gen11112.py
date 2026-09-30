from pyspark.sql import functions as F

src = (
    df_inventory
    .filter(F.col("product_id").isin([4, 5, 9]) & F.col("store_id").isin([2, 5, 6]))
)

inventory_stats = (
    src
    .groupBy(F.month("last_restock").alias("last_restock_month"), "product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.min("last_restock").alias("min_last_restock"),
    )
)

result = (
    inventory_stats
    .filter(F.col("min_quantity") > 120)
    .orderBy(F.col("last_restock_month").desc())
)
