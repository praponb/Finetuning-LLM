from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.store_id").isin([2, 6]))
    .groupBy(
        F.col("ss.product_id"),
        F.when(F.col("ss.revenue") >= 3600, "high").when(F.col("ss.revenue") >= 1100, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(F.max(F.col("i.quantity")).alias("max_quantity"))
    .filter(F.col("max_quantity") >= 180)
)
