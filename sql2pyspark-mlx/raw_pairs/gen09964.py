from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.units") != 26)
    .groupBy(
        F.when(F.col("ss.revenue") >= 3300, "high").when(F.col("ss.revenue") >= 600, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.min(F.col("i.quantity")).alias("min_quantity"),
    )
    .orderBy("quantity_count")
)
