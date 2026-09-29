from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .select(
        F.col("inventory.product_id"),
        F.col("products.product_name"),
        F.col("suppliers.rating"),
        F.when(F.col("suppliers.rating") >= 4.4, "High").when(F.col("suppliers.rating") >= 1.6, "Medium").otherwise("Low").alias("rating_level"),
    )
)
