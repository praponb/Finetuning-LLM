from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 9]))
    .select(
        "id",
        "last_restock",
        "store_id",
        F.dayofweek("last_restock").alias("last_restock_dayofweek"),
    )
)
