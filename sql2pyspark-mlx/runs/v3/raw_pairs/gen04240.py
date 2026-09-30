from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 5, 6, 10]))
    .select("last_restock")
    .subtract(
        df_inventory
        .filter(F.col("last_restock") > "2024-04-19")
        .select("last_restock")
    )
)
