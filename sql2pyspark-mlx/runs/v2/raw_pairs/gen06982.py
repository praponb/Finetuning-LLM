from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 5]) & (F.col("last_restock") > "2023-12-13"))
    .select(
        "id",
        "product_id",
        "last_restock",
        "store_id",
        F.floor("quantity").alias("quantity_whole"),
        F.round((F.col("quantity") * 1.15), 2).alias("adjusted_quantity"),
    )
)
