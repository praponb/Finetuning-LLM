from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .groupBy(
        F.when(F.col("ss.units") >= 43, "high").when(F.col("ss.units") >= 18, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.count("*").alias("num_inventory"),
        F.countDistinct(F.col("i.quantity")).alias("unique_products"),
    )
    .orderBy(F.col("unique_products").desc())
)
