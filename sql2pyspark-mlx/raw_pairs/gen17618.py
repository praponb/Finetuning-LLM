from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "left")
    .select(
        F.col("inventory.id"),
        F.col("inventory.last_restock"),
        F.col("products.cost"),
    )
    .orderBy("cost")
)
