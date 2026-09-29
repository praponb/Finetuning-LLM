from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") != 237) & (F.col("product_id").isin([1, 2, 5, 6]) | (F.year("last_restock") == 2025)))
    .select(
        "id",
        "quantity",
        "product_id",
        "store_id",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
    .orderBy("store_id", "id")
    .limit(50)
)
