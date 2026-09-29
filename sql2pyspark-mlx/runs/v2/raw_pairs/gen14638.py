from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([6, 8, 11]))
    .select(
        "quantity",
        "product_id",
        F.col("quantity").cast("string").alias("quantity_str"),
    )
)
