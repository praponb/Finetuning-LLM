from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter((F.col("i.last_restock") < "2024-09-01") & (F.col("ss.product_id") == 1))
    .select(
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("ss.sale_date"),
        F.col("ss.revenue"),
        F.col("ss.store_id"),
    )
)
