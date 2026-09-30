from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 5]) & F.col("product_id").isin([1, 3, 4, 6]))
    .select(
        "store_id",
        (F.datediff(F.current_date(), "last_restock") / 365).alias("years_since_last_restock"),
    )
)
