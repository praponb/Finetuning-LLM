from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "last_restock",
        "store_id",
        "quantity",
        F.date_add("last_restock", 31).alias("last_restock_plus_31d"),
    )
    .orderBy(F.col("id").desc())
)
