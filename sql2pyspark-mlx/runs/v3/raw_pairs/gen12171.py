from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2024-03-01", "2024-03-30") & (F.col("store_id") == 6))
    .select(
        "product_id",
        "store_id",
        "quantity",
        F.date_format("last_restock", "MMM yyyy").alias("last_restock_label"),
    )
)
