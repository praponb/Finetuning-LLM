from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "right")
    .filter(F.col("products.stock_qty") <= 192)
    .groupBy(F.col("products.brand"), F.col("products.category"))
    .agg(
        (F.max(F.col("inventory.quantity")) - F.min(F.col("inventory.quantity"))).alias("quantity_range"),
        F.count(F.col("inventory.quantity")).alias("quantity_count"),
    )
)
