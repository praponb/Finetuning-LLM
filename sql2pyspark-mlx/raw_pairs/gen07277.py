from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") > "2025-05-17")
    .select("id", "store_id", "last_restock")
    .unionByName(
        df_inventory
        .filter(F.col("quantity") != 60)
        .select("id", "store_id", "last_restock")
    )
    .distinct()
)
