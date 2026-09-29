from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("store_id") == 6) & F.col("product_id").isin([8, 10]) & F.col("product_id").isin([5, 10]))
    .select(
        "product_id",
        "quantity",
        "last_restock",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)
