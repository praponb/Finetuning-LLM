from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.revenue") < 3000)
    .groupBy(
        F.when(F.col("ss.units") >= 33, "high").when(F.col("ss.units") >= 12, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.min(F.col("i.quantity")).alias("min_quantity"),
    )
    .orderBy(F.col("units_bucket").desc(), "min_quantity")
)
