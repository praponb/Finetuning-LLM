from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "inner")
    .groupBy(
        F.col("products.brand"),
        F.when(F.col("products.category").isin(["Fashion", "Home"]), "tier_1").otherwise("other").alias("category_group"),
    )
    .agg(F.min(F.col("inventory.quantity")).alias("min_quantity"))
    .orderBy("category_group")
)
