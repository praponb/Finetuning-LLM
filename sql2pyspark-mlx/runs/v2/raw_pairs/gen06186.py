from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") != 61)
    .select("id", "last_restock", "store_id")
    .unionByName(
        df_inventory
        .filter(F.col("last_restock") >= "2025-01-20")
        .select("id", "last_restock", "store_id")
    )
    .orderBy("last_restock")
)
