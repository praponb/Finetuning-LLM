from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") <= 141)
    .select("store_id", "product_id")
    .intersect(
        df_inventory
        .filter(F.col("store_id").isin([1, 2, 5]))
        .select("store_id", "product_id")
    )
)
