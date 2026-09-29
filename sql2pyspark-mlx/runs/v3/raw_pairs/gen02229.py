from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 4, 6]) & (F.col("store_id") == 3))
    .select(
        "id",
        "quantity",
        "store_id",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
        F.round((F.col("quantity") * 1.07), 2).alias("quantity_adjusted"),
    )
)
