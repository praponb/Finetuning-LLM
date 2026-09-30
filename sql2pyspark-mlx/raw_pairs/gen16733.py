from pyspark.sql import functions as F

valid = (
    df_inventory
    .filter(F.col("store_id") == 2)
    .select(
        "id",
        "store_id",
        "last_restock",
        (F.datediff(F.current_date(), "last_restock") / 7).alias("weeks_since_last_restock"),
    )
)

result = (
    valid
    .select("store_id")
    .subtract(
        df_inventory
        .filter(F.col("last_restock").between("2023-06-01", "2025-04-18"))
        .select("store_id")
    )
)
