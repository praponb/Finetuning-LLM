from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") > "2024-01-01")
    .select("last_restock")
    .unionByName(
        df_inventory
        .filter(F.col("last_restock").between("2024-07-15", "2025-08-18"))
        .select("last_restock")
    )
)
