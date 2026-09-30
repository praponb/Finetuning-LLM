from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") != 87)
    .select("product_id")
    .unionByName(
        df_inventory
        .filter(F.col("last_restock") > "2025-03-04")
        .select("product_id")
    )
    .distinct()
)
