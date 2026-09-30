from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id") == 6)
    .select("id", "last_restock")
    .subtract(
        df_inventory
        .filter(F.col("last_restock") < "2024-10-01")
        .select("id", "last_restock")
    )
)
