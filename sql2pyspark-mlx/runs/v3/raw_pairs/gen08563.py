from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([2, 5, 9]) & F.col("store_id").isin([2, 4, 5]))
    .select(
        "id",
        "product_id",
        "store_id",
        "quantity",
        F.col("quantity").cast("string").alias("quantity_str"),
    )
)
