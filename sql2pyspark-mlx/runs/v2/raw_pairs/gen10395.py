from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .filter((F.col("store_sales.sale_date") >= "2025-05-01") & (F.col("inventory.quantity") > 204))
    .select(
        F.col("inventory.id"),
        F.col("inventory.last_restock"),
        F.col("inventory.product_id"),
        F.col("store_sales.product_id").alias("store_sale_product_id"),
        F.col("store_sales.units"),
        F.col("store_sales.store_id"),
        F.when(F.col("store_sales.revenue") >= 3400, "High").when(F.col("store_sales.revenue") >= 1100, "Medium").otherwise("Low").alias("revenue_band"),
    )
    .orderBy(F.col("id").desc())
)
