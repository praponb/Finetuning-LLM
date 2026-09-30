from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .groupBy(
        F.col("ss.product_id"),
        F.date_trunc("MONTH", F.col("ss.sale_date")).alias("sale_month"),
    )
    .agg(F.sum(F.col("i.quantity")).alias("total_quantity"))
    .orderBy("sale_month")
)
