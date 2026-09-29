from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([6, 7]))
    .select(
        "id",
        "quantity",
        "store_id",
        "last_restock",
        F.round("quantity", 0).alias("quantity_rounded"),
    )
)
