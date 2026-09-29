from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.store_id").isin([1, 2, 3, 6]))
    .groupBy(
        F.col("ss.product_id"),
        F.date_trunc("MONTH", F.col("ss.sale_date")).alias("sale_month"),
    )
    .agg(F.max(F.col("i.last_restock")).alias("latest_last_restock"))
)
