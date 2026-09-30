from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("product_id").isin([5, 7, 8, 9]) | F.col("store_id").isin([1, 3, 5, 6]))
)

inventory_stats = (
    base
    .groupBy("product_id", "store_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    inventory_stats
    .filter(F.col("quantity_count") > 5)
    .select("product_id", "store_id", "avg_quantity")
    .orderBy(F.col("store_id").desc(), "product_id")
    .limit(20)
)
