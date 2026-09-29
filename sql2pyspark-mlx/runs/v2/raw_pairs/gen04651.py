from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock").between("2024-01-28", "2025-01-01") & F.col("quantity").isNotNull()) | (F.col("quantity") >= 83))
    .select(
        "store_id",
        "product_id",
        "last_restock",
        F.round((F.col("quantity") * 0.9), 2).alias("adjusted_quantity"),
    )
)
