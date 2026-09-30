from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 4, 6]))
    .select("product_id", "last_restock")
    .unionByName(
        df_inventory
        .filter(F.col("product_id") == 2)
        .select("product_id", "last_restock")
    )
)
