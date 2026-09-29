from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.product_id").isin([4, 6]))
    .groupBy(F.col("ss.store_id"))
    .agg(F.max(F.col("i.quantity")).alias("max_quantity"))
    .orderBy("max_quantity", F.col("store_id").desc())
)
