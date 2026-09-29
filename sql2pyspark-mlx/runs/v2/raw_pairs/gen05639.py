from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.product_id").isin([1, 6, 8, 9]))
    .groupBy(
        F.when(F.col("ss.revenue") >= 4000, "high").when(F.col("ss.revenue") >= 900, "mid").otherwise("low").alias("revenue_bucket"),
        F.col("ss.product_id"),
    )
    .agg(
        F.countDistinct(F.col("i.quantity")).alias("unique_products"),
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
    )
    .filter(F.col("unique_products") > 1)
    .orderBy(F.col("product_id").desc(), "revenue_bucket")
)
