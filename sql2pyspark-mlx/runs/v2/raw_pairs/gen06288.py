from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") >= 215) & F.col("store_id").isin([1, 4, 6]) & F.col("last_restock").between("2024-09-01", "2025-07-01"))
    .groupBy("product_id", F.year("last_restock").alias("last_restock_year"))
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.avg("quantity").alias("avg_quantity"),
        F.min("last_restock").alias("first_last_restock"),
    )
    .filter(F.col("total_quantity") >= 570)
    .orderBy("first_last_restock")
)
