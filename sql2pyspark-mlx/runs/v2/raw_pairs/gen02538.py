from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .filter(F.col("store_sales.revenue").between(2200, 2600) & F.col("inventory.quantity").between(76, 201))
    .select(
        F.col("inventory.product_id"),
        F.col("store_sales.revenue"),
        (F.col("store_sales.revenue") * F.col("store_sales.units")).alias("revenue_per_unit"),
    )
    .orderBy(F.col("product_id").desc(), F.col("revenue").desc())
)
