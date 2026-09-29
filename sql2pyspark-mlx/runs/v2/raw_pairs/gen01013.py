from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2024-05-11", "2025-04-01") & ((F.col("quantity") > 160) | F.col("store_id").isin([1, 2, 3, 4])))
    .groupBy(F.month("last_restock").alias("last_restock_month"), "store_id")
    .agg(
        F.max("last_restock").alias("last_last_restock"),
        F.min("quantity").alias("min_quantity"),
    )
    .orderBy("last_restock_month")
)
