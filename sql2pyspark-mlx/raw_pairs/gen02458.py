from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "store_id",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
    )
)
