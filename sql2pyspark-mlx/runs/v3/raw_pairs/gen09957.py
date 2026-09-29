from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "outer")
    .groupBy(F.col("products.category"))
    .agg(F.count(F.col("inventory.quantity")).alias("quantity_count"))
)
