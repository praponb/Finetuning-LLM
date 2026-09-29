from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "quantity",
        F.floor("quantity").alias("quantity_whole"),
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
    )
)
