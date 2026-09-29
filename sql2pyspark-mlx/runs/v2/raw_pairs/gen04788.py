from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.revenue") <= 2200)
    .groupBy(
        F.when(F.col("ss.revenue") >= 3900, "high").when(F.col("ss.revenue") >= 1100, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
    )
    .orderBy(F.col("total_quantity").desc())
)
