from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .filter(F.col("inventory.quantity").between(139, 152) & F.col("inventory.last_restock").between("2023-05-01", "2024-01-01"))
    .select(
        F.col("inventory.store_id"),
        F.col("store_sales.revenue"),
        F.col("store_sales.units"),
    )
    .orderBy(F.col("revenue").desc(), F.col("units").desc())
)
