from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") <= 217)
    .select("quantity")
    .unionByName(
        df_inventory
        .filter(F.col("last_restock") <= "2024-10-01")
        .select("quantity")
    )
    .distinct()
)
