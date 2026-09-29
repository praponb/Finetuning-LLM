from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.store_id").isin([3, 4, 5, 6]))
    .groupBy(
        F.when(F.col("ss.units") >= 44, "high").when(F.col("ss.units") >= 7, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        F.count("*").alias("inventory_count"),
    )
    .orderBy(F.col("total_quantity").desc())
)
