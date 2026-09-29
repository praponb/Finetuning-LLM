from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") != 173) & (F.col("store_id") == 4))
    .select(
        "product_id",
        "quantity",
        F.add_months("last_restock", 12).alias("last_restock_plus_months"),
    )
)
