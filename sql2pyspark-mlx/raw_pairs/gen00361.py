from pyspark.sql import functions as F

filtered_inventory = (
    df_inventory
    .filter((F.col("store_id") == 3) & ((F.col("last_restock") <= "2023-06-28") | F.col("last_restock").between("2024-01-01", "2025-06-01")))
)

agg = (
    filtered_inventory
    .groupBy("store_id")
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.count("*").alias("total_count"),
    )
)

result = agg.orderBy(F.col("total_count").desc())
