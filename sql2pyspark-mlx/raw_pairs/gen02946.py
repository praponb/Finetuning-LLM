from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .groupBy(
        F.date_format(F.col("store_sales.sale_date"), "yyyy-MM").alias("sale_month"),
    )
    .agg(F.min(F.col("inventory.quantity")).alias("min_quantity"))
    .filter(F.col("min_quantity") >= 230)
    .orderBy("sale_month")
)
