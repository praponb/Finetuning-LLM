from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 6]))
    .select(
        "id",
        "product_id",
        "last_restock",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
    .orderBy(F.col("product_id").desc(), "id")
    .limit(5)
)
