from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.quantity") == 137)
    .groupBy(
        F.when(F.col("ss.revenue") >= 4100, "high").when(F.col("ss.revenue") >= 800, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(F.min(F.col("i.last_restock")).alias("min_last_restock"))
)
