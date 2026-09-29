from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~(F.col("last_restock").between("2024-07-01", "2025-01-01") & (F.col("quantity") >= 233)))
    .select(
        "last_restock",
        "store_id",
        F.year("last_restock").alias("last_restock_year"),
    )
    .orderBy(F.col("store_id").desc(), F.col("last_restock_year").desc())
)
