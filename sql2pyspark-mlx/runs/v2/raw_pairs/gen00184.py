from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("product_id").isin([2, 3, 5, 9]) & F.col("quantity").between(89, 134))
)

inventory_stats = (
    base
    .groupBy("store_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = inventory_stats.select("store_id", "quantity_range", "max_quantity")
