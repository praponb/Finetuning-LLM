from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 5]))
    .select("id", "quantity")
    .subtract(
        df_inventory
        .filter(F.col("last_restock") >= "2025-04-01")
        .select("id", "quantity")
    )
)
