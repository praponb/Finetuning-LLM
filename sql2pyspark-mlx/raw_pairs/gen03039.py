from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "quantity",
        "store_id",
        "last_restock",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
    )
    .orderBy("last_restock")
)
