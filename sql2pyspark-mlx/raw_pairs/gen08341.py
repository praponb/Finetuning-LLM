from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.month(F.col("i.last_restock")).isin([3, 7, 12]))
    .select(
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("ss.product_id"),
        F.col("ss.units"),
        F.col("ss.revenue"),
    )
)
