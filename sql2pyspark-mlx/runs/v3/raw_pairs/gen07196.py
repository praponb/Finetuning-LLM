from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .filter(F.year(F.col("inventory.last_restock")) >= 2023)
    .groupBy(
        F.when(F.col("stores.city").isin(["London", "Berlin"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(
        F.min(F.col("inventory.last_restock")).alias("first_last_restock"),
        (F.max(F.col("inventory.quantity")) - F.min(F.col("inventory.quantity"))).alias("quantity_range"),
    )
    .orderBy(F.col("quantity_range").desc(), "first_last_restock")
)
