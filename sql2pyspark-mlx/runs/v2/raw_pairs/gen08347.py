from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 5]))
    .select(
        "id",
        "quantity",
        "last_restock",
        F.round((F.col("quantity") * 100), 2).alias("adjusted_quantity"),
    )
)
