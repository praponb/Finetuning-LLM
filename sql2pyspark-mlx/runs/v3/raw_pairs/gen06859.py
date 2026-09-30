from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.product_id").isin([3, 5, 6, 10]))
    .groupBy(
        F.when(F.col("ss.revenue") >= 3900, "high").when(F.col("ss.revenue") >= 1000, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.max(F.col("i.quantity")).alias("max_quantity"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
    )
    .orderBy(F.col("max_quantity").desc(), "revenue_bucket")
    .limit(3)
)
