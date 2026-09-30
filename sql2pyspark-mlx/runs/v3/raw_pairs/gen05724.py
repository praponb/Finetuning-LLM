from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 6)
    .select("id", "quantity")
    .unionByName(
        df_inventory
        .filter(F.col("last_restock") > "2025-06-01")
        .select("id", "quantity")
    )
)
