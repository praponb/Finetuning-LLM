from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .groupBy(
        F.when(F.col("store_sales.units") >= 36, "high").when(F.col("store_sales.units") >= 7, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(F.max(F.col("inventory.quantity")).alias("max_quantity"))
    .orderBy("units_bucket")
    .limit(1)
)
