from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .filter(F.col("store_sales.units") == 28)
    .groupBy(F.col("store_sales.product_id"))
    .agg(F.count("*").alias("num_inventory"))
    .orderBy("num_inventory")
)
