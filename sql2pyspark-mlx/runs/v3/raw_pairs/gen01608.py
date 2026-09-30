from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2025-06-13", "2025-10-01") & ((F.col("last_restock") <= "2025-08-01") | (F.col("quantity") <= 109)))
    .groupBy(
        F.month("last_restock").alias("last_restock_month"),
        F.when(F.col("quantity") >= 265, "high").when(F.col("quantity") >= 106, "mid").otherwise("low").alias("quantity_bucket"),
        "store_id",
    )
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.min("last_restock").alias("min_last_restock"),
    )
    .filter(F.col("quantity_count") < 1)
    .orderBy(F.col("quantity_count").desc())
)
