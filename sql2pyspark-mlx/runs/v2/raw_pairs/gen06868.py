from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.store_id").isin([2, 3, 4, 5]))
    .groupBy(F.col("ss.product_id"))
    .agg(F.countDistinct(F.col("i.quantity")).alias("unique_stores"))
    .filter(F.col("unique_stores") > 3)
)
