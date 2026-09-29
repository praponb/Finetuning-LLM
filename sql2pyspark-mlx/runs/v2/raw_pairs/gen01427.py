from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.product_id").isin([1, 7]))
    .groupBy(
        F.date_trunc("MONTH", F.col("ss.sale_date")).alias("sale_month"),
        F.col("ss.product_id"),
    )
    .agg(
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
    )
    .orderBy("avg_quantity", "product_id", "sale_month")
    .limit(1)
)
