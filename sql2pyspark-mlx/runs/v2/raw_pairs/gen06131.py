from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.units") < 21)
    .groupBy(F.date_trunc("MONTH", F.col("ss.sale_date")).alias("sale_month"))
    .agg(F.min(F.col("i.last_restock")).alias("min_last_restock"))
    .orderBy(F.col("sale_month").desc(), "min_last_restock")
)
