from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") <= "2023-06-24")
    .select("id", "last_restock", "product_id")
    .unionByName(
        df_inventory
        .filter(F.col("store_id") == 3)
        .select("id", "last_restock", "product_id")
    )
)
