from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.product_id") == 10)
    .groupBy(F.date_format(F.col("ss.sale_date"), "yyyy-MM").alias("sale_month"))
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
    )
    .orderBy("total_quantity", F.col("avg_quantity").desc(), "sale_month")
    .limit(3)
)
