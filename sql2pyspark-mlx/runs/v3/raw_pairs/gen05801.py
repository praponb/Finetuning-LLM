from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "last_restock",
        "store_id",
        F.date_add("last_restock", 88).alias("last_restock_plus_88d"),
    )
)
