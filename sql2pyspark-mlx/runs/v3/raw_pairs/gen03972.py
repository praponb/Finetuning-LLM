from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") < "2024-04-01")
    .select("id", "last_restock", "product_id")
    .unionByName(
        df_inventory
        .filter(F.col("product_id").isin([4, 5, 7, 9]))
        .select("id", "last_restock", "product_id")
    )
)
