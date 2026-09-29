from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "quantity",
        "product_id",
        "store_id",
        F.date_format("last_restock", "MMM yyyy").alias("last_restock_label"),
    )
)
