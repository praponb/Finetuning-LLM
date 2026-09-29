from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "store_id",
        "quantity",
        F.date_format("last_restock", "MMM yyyy").alias("last_restock_label"),
    )
    .orderBy("quantity")
)
