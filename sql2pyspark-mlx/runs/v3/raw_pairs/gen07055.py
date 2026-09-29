from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.alias("inventory"), F.col("products.id") == F.col("inventory.product_id"), "left")
    .select(
        F.col("products.launch_date"),
        F.col("inventory.last_restock"),
        F.array_join(F.col("products.tags"), ", ").alias("tags_text"),
    )
    .orderBy("launch_date")
)
