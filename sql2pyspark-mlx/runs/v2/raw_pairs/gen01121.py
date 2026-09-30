from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.col("products.product_name").like("%Nois%"))
    .groupBy(
        F.when(F.col("suppliers.rating") >= 3.9, "high").when(F.col("suppliers.rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        (F.max(F.col("inventory.quantity")) - F.min(F.col("inventory.quantity"))).alias("quantity_range"),
    )
    .filter(F.col("quantity_range") >= 26)
    .orderBy("rating_bucket", "quantity_range")
)
