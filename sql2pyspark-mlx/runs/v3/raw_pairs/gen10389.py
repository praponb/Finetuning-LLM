from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 3, 5, 6]) & F.col("product_id").isin([8, 9]))
    .select(
        "id",
        "product_id",
        "store_id",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)
