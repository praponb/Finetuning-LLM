from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.alias("inventory"), F.col("products.id") == F.col("inventory.product_id"), "left")
    .filter(F.col("inventory.quantity").between(218, 225))
    .groupBy(F.col("products.id"), F.col("products.brand"))
    .agg(
        F.count(F.col("inventory.id")).alias("inventory_count"),
        F.min(F.col("inventory.quantity")).alias("min_quantity"),
        F.count(F.col("inventory.quantity")).alias("quantity_count"),
    )
    .orderBy(F.col("id").desc())
)
