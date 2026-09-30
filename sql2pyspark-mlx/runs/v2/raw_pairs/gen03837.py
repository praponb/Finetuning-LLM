from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 4, 6]))
    .select("last_restock", "product_id")
    .unionByName(
        df_inventory
        .filter(F.col("product_id").isin([1, 8, 10]))
        .select("last_restock", "product_id")
    )
    .orderBy("last_restock")
)
