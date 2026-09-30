from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") > "2025-05-16")
    .select(
        "id",
        "last_restock",
        "quantity",
        F.round("quantity", 2).alias("quantity_rounded"),
    )
)
