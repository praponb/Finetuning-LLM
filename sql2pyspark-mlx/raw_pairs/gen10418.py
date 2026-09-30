from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.product_id").isin([3, 6, 9]))
    .groupBy(
        F.col("ss.product_id"),
        F.when(F.col("ss.revenue") >= 3300, "high").when(F.col("ss.revenue") >= 2000, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(F.max(F.col("i.last_restock")).alias("last_last_restock"))
)
