from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 4, 5]))
    .select("last_restock")
    .intersect(
        df_inventory
        .filter(F.col("store_id").isin([2, 3, 4]))
        .select("last_restock")
    )
)
