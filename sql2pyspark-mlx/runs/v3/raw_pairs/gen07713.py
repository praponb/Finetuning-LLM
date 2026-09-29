from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 4, 5, 6]))
    .select("id", "quantity")
    .subtract(
        df_inventory
        .filter(F.col("last_restock") < "2023-05-01")
        .select("id", "quantity")
    )
)
