from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        F.col("quantity").cast("string").alias("quantity_num"),
        F.date_add("last_restock", 56).alias("last_restock_plus_56d"),
    )
)
