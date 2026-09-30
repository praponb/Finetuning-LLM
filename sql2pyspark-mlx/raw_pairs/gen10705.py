from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") <= 82) & F.col("store_id").isin([3, 4, 5]))
    .select(
        "last_restock",
        "quantity",
        "product_id",
        (F.datediff(F.current_date(), "last_restock") / 7).alias("weeks_since_last_restock"),
        F.round((F.col("quantity") * 0.9), 2).alias("quantity_adjusted"),
    )
)
