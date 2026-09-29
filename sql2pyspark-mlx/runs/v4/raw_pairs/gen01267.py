from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .groupBy(
        F.when(F.col("ss.revenue") >= 3800, "high").when(F.col("ss.revenue") >= 900, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("stddev_quantity").desc(), "revenue_bucket")
)
