from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(74, 125))
    .select(
        "id",
        "quantity",
        "store_id",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)
