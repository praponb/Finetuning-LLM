from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.product_id").isin([3, 5, 6]))
    .groupBy(
        F.when(F.col("ss.units") >= 43, "high").when(F.col("ss.units") >= 7, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(F.count(F.col("i.quantity")).alias("quantity_count"))
    .orderBy(F.col("quantity_count").desc(), "units_bucket")
    .limit(10)
)
