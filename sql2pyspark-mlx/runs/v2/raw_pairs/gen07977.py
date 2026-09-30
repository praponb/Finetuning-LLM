from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter((F.col("ss.revenue") <= 1800) & (F.year(F.col("i.last_restock")) == 2025))
    .select(
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("ss.product_id").alias("store_sale_product_id"),
        F.col("ss.revenue"),
    )
)
