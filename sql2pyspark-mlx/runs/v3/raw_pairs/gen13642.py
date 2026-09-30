from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.store_id") == 5)
    .groupBy(F.col("ss.store_id"), F.col("ss.product_id"))
    .agg(F.min(F.col("i.quantity")).alias("min_quantity"))
    .filter(F.col("min_quantity") > 160)
    .orderBy(F.col("store_id").desc(), "product_id")
    .limit(1)
)
