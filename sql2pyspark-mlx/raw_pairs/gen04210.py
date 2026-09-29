from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .filter(F.col("inventory.store_id").isin([3, 4, 6]))
    .groupBy(
        F.when(F.col("suppliers.rating") >= 3.7, "high").when(F.col("suppliers.rating") >= 1.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count(F.col("inventory.quantity")).alias("quantity_count"))
    .orderBy("rating_bucket")
)
