from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 3, 6]))
    .select(
        "id",
        "last_restock",
        "product_id",
        F.round((F.col("quantity") * 0.85), 2).alias("quantity_adjusted"),
    )
)
