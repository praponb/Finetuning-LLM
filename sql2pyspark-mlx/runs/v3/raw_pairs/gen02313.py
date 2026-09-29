from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2024-10-24", "2025-01-23"))
    .select("id", "store_id")
    .subtract(
        df_inventory
        .filter(F.col("last_restock") <= "2025-02-14")
        .select("id", "store_id")
    )
    .orderBy("id")
)
