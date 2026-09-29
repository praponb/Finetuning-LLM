from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.units") == 18)
    .groupBy(
        F.col("ss.store_id"),
        F.when(F.col("ss.units") >= 44, "high").when(F.col("ss.units") >= 20, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
        F.count(F.col("i.quantity")).alias("quantity_count"),
    )
)
