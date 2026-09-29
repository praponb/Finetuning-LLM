from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "last_restock",
        "store_id",
        "product_id",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
        F.round((F.col("quantity") * 0.85), 2).alias("quantity_scaled"),
    )
    .orderBy("last_restock_label")
)
