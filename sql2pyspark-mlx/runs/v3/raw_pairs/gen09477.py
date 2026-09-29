from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.month(F.col("i.last_restock")).isin([5, 7, 8]))
    .groupBy(
        F.when(F.col("ss.revenue") >= 3400, "high").when(F.col("ss.revenue") >= 500, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("row_count").desc(), F.col("revenue_bucket").desc())
)
