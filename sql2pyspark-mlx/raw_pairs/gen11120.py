from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.revenue") > 2800)
    .groupBy(
        F.when(F.col("ss.revenue") >= 3300, "high").when(F.col("ss.revenue") >= 1200, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.countDistinct(F.col("i.quantity")).alias("unique_stores"),
        F.count("*").alias("cnt"),
    )
)
