from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 3, 4]))
    .select(
        "quantity",
        "last_restock",
        F.col("quantity").cast("string").alias("quantity_num"),
    )
)
