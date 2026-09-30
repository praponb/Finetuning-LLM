from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.product_id").isin([1, 6]))
    .groupBy(
        F.col("ss.product_id"),
        F.date_format(F.col("ss.sale_date"), "yyyy-MM").alias("sale_month"),
    )
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        F.max(F.col("i.quantity")).alias("max_quantity"),
    )
)
