from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("store_id") == 6) & (F.col("last_restock") < "2025-03-01"))
    .select(
        "id",
        "last_restock",
        "quantity",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)
