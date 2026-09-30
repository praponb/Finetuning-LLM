from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.revenue") >= 3900)
    .groupBy(
        F.col("ss.store_id"),
        F.when(F.col("ss.units") >= 43, "high").when(F.col("ss.units") >= 9, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
    )
    .orderBy(F.col("quantity_range").desc(), F.col("store_id").desc())
)
