from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "left")
    .filter(F.col("store_sales.store_id").isin([1, 3]))
    .groupBy(
        F.col("store_sales.store_id"),
        F.date_format(F.col("store_sales.sale_date"), "yyyy-MM").alias("sale_month"),
    )
    .agg(F.max(F.col("inventory.quantity")).alias("max_quantity"))
)
