from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "left")
    .filter((F.col("inventory.store_id") == 1) & F.col("store_sales.product_id").isin([3, 10]))
    .select(
        F.col("inventory.id"),
        F.col("inventory.quantity"),
        F.col("inventory.product_id"),
        F.col("store_sales.sale_date"),
        F.col("store_sales.units"),
        F.round((F.col("store_sales.revenue") / F.col("store_sales.units")), 2).alias("revenue_per_unit"),
    )
    .orderBy(F.col("revenue_per_unit").desc(), F.col("units").desc())
)
