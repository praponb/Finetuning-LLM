from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2024-01-21", "2024-03-01"))
    .select(
        "id",
        "store_id",
        (F.datediff(F.current_date(), "last_restock") / 365).alias("years_since_last_restock"),
    )
)
