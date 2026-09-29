from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") >= "2023-08-26")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("inventory_count"),
    )
    .orderBy(F.col("inventory_count").desc())
)
