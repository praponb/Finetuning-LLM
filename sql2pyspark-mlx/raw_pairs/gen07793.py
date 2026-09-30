from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .filter(F.col("stores.opened_date") > "2024-05-26")
    .groupBy(F.col("products.brand"))
    .agg(
        F.avg(F.col("inventory.quantity")).alias("avg_quantity"),
        (F.max(F.col("inventory.quantity")) - F.min(F.col("inventory.quantity"))).alias("quantity_range"),
    )
    .orderBy(F.col("avg_quantity").desc())
)
