from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .filter((F.col("inventory.store_id") == 6) & (F.col("products.cost") >= 650))
    .select(
        F.col("inventory.id"),
        F.col("inventory.product_id"),
        F.col("inventory.quantity"),
        F.col("products.cost"),
    )
    .orderBy("quantity")
)
