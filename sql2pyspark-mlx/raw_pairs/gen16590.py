from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .groupBy(F.col("suppliers.city"))
    .agg(
        F.round(F.avg(F.col("inventory.quantity")), 2).alias("avg_quantity"),
        F.min(F.col("inventory.last_restock")).alias("min_last_restock"),
    )
)
