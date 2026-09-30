from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.alias("inventory"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .filter(~(F.col("inventory.store_id").isin([1, 2, 3, 4]) | (F.col("inventory.store_id") == 4)))
    .select(
        F.col("products.cost"),
        F.col("inventory.store_id"),
        F.col("inventory.product_id"),
        F.col("inventory.quantity"),
    )
)
