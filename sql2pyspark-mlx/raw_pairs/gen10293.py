from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") != 122) & F.col("store_id").isin([1, 4]))
    .select(
        "last_restock",
        "store_id",
        "quantity",
        F.col("quantity").cast("double").alias("quantity_str"),
    )
)
