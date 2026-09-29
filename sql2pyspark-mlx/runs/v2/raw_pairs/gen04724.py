from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") == 133) & (F.col("last_restock") <= "2024-09-01"))
    .select(
        "id",
        "last_restock",
        "store_id",
        "quantity",
        F.round((F.col("quantity") * 0.85), 2).alias("adjusted_quantity"),
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
    )
)
