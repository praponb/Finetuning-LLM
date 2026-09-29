from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.year("last_restock") == 2024)
    .select(
        "id",
        "quantity",
        "last_restock",
        "store_id",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
    .orderBy(F.col("store_id").desc())
)
