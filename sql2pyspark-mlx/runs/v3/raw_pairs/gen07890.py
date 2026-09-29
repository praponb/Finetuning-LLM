from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id") == 6)
    .select("quantity", "last_restock")
    .subtract(
        df_inventory
        .filter(F.col("product_id").isin([2, 5]))
        .select("quantity", "last_restock")
    )
)
