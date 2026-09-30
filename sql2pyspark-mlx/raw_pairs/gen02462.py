from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .groupBy(
        F.col("ss.store_id"),
        F.when(F.col("ss.revenue") >= 4100, "high").when(F.col("ss.revenue") >= 1900, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.countDistinct(F.col("i.quantity")).alias("unique_products"),
    )
    .orderBy(F.col("unique_products").desc(), "store_id", "revenue_bucket")
    .limit(50)
)
