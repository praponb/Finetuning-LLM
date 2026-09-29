from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 4, 5]) & (F.col("quantity") <= 197))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_format("last_restock", "MMM yyyy").alias("last_restock_label"),
    )
)
