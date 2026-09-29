from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2024-10-01", "2025-01-01") & F.col("quantity").between(62, 219) & F.col("product_id").isin([1, 4, 8, 10]))
    .select(
        "id",
        "product_id",
        "store_id",
        F.add_months("last_restock", 3).alias("last_restock_plus_months"),
    )
)
