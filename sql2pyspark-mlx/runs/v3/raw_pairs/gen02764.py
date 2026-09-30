from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 4]))
    .select("id", "product_id", "last_restock")
    .unionByName(
        df_inventory
        .filter(F.col("last_restock") >= "2025-09-12")
        .select("id", "product_id", "last_restock")
    )
    .orderBy(F.col("product_id").desc())
)
