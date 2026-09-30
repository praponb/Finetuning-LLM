from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2]) & F.col("store_id").isin([2, 4, 6]) & F.month("last_restock").isin([2, 3, 8]))
)

inventory_stats = (
    base
    .groupBy(
        "product_id",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.min("last_restock").alias("first_last_restock"),
    )
)

result = (
    inventory_stats
    .filter(F.col("max_quantity") > 160)
    .select("product_id", "last_restock_month", "max_quantity", "first_last_restock")
    .orderBy(F.col("last_restock_month").desc())
)
