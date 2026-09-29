from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .groupBy(
        F.when(F.col("suppliers.rating") >= 4.2, "high").when(F.col("suppliers.rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.min(F.col("inventory.last_restock")).alias("min_last_restock"))
    .orderBy("rating_bucket")
)
