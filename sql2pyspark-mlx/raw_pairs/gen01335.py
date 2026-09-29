from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 6)
    .select("id", "store_id", "last_restock")
    .subtract(
        df_inventory
        .filter(F.col("product_id").isin([4, 5, 6, 10]))
        .select("id", "store_id", "last_restock")
    )
)
