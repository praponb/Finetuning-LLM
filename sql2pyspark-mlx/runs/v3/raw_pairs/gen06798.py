from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("i.product_id").isin([4, 8, 9]) | F.col("i.product_id").isin([1, 9]))
    .select(
        F.col("s.city"),
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("i.quantity"),
    )
)
