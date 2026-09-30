from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 9]))
    .select(
        "store_id",
        "product_id",
        "quantity",
        F.add_months("last_restock", 1).alias("last_restock_plus_months"),
    )
)
