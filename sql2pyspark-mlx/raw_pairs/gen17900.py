from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~((F.col("store_id") == 1) & F.col("quantity").isNotNull() & F.col("last_restock").between("2025-03-01", "2025-10-09")))
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.min("last_restock").alias("min_last_restock"),
    )
    .orderBy(F.col("min_quantity").desc())
)
