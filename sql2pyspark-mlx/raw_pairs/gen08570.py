from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([4, 5, 6]))
    .select("id", "last_restock", "product_id")
    .unionByName(
        df_inventory
        .filter(F.col("store_id") == 5)
        .select("id", "last_restock", "product_id")
    )
)
