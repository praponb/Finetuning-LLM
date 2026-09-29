from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id") == 6)
    .select(
        "quantity",
        "product_id",
        F.date_add("last_restock", 49).alias("last_restock_plus_49d"),
    )
    .orderBy(F.col("quantity").desc())
)
