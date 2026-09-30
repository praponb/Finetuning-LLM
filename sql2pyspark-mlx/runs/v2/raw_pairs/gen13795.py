from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter((F.col("last_restock").between("2023-09-13", "2025-01-15") & F.col("product_id").isin([1, 3, 9])) | (F.col("product_id") == 7))
)

summary = (
    valid_inventory
    .groupBy("store_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = summary.select("store_id", "min_quantity")
