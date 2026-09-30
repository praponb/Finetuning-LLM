from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.store_id").isin([1, 3]) & (F.col("i.product_id") == 2))
    .select(
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("ss.store_id").alias("store_sale_store_id"),
        F.col("ss.sale_date"),
        F.col("ss.revenue"),
    )
)
