from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
        F.col("quantity").cast("string").alias("quantity_str"),
    )
    .orderBy("last_restock_label")
)
