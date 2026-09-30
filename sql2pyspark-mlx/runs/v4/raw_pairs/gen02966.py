from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "quantity",
        "store_id",
        "product_id",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
        F.round((F.col("quantity") * 100), 2).alias("quantity_scaled"),
    )
)
