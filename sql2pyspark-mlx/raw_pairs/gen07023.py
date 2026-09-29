from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.last_restock") < "2024-11-01")
    .groupBy(F.date_trunc("MONTH", F.col("ss.sale_date")).alias("sale_month"))
    .agg(F.count(F.col("i.quantity")).alias("quantity_count"))
)
