from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.product_id").isin([5, 6]))
    .groupBy(
        F.when(F.col("ss.revenue") >= 4200, "high").when(F.col("ss.revenue") >= 800, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.max(F.col("i.quantity")).alias("max_quantity"),
        F.count(F.col("i.quantity")).alias("quantity_count"),
    )
    .orderBy(F.col("revenue_bucket").desc())
)
