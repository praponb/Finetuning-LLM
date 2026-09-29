from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.store_id").isin([5, 6]))
    .groupBy(
        F.when(F.col("ss.units") >= 37, "high").when(F.col("ss.units") >= 17, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.min(F.col("i.quantity")).alias("min_quantity"),
        F.min(F.col("i.last_restock")).alias("first_last_restock"),
    )
)
