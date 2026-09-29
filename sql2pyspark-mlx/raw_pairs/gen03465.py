from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2024-01-01", "2025-01-01"))
    .groupBy("store_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .filter(F.col("quantity_range") > 68)
    .select("store_id", "total_quantity", "quantity_range")
)
