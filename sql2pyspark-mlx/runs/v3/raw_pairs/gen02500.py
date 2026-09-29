from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.store_id") == 1)
    .groupBy(
        F.when(F.col("ss.units") >= 37, "high").when(F.col("ss.units") >= 8, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
        F.count(F.col("i.quantity")).alias("quantity_count"),
    )
    .orderBy("units_bucket")
    .limit(50)
)
