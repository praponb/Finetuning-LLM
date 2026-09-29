from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .filter(F.year(F.col("store_sales.sale_date")) == 2024)
    .groupBy(F.col("store_sales.store_id"))
    .agg(
        F.countDistinct(F.col("inventory.quantity")).alias("unique_products"),
        F.avg(F.col("inventory.quantity")).alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") > 230)
)
