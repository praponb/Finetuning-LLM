from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 5]) & F.month("last_restock").isin([1, 8, 9]))
    .select(
        "id",
        "quantity",
        "product_id",
        "store_id",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
    .orderBy(F.col("store_id").desc(), F.col("days_since_last_restock").desc())
)
