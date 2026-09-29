from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 4, 6]))
    .select("store_id", "quantity")
    .unionByName(
        df_inventory
        .filter(F.col("last_restock") < "2025-07-01")
        .select("store_id", "quantity")
    )
    .distinct()
)
