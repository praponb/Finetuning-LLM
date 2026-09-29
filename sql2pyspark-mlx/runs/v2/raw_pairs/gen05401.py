from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 3)
    .select("id", "product_id")
    .unionByName(
        df_inventory
        .filter(F.col("last_restock") <= "2025-01-23")
        .select("id", "product_id")
    )
    .distinct()
)
