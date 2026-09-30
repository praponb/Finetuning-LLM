from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.store_id") == 2)
    .groupBy(
        F.date_trunc("MONTH", F.col("ss.sale_date")).alias("sale_month"),
        F.col("ss.store_id"),
    )
    .agg(F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"))
    .filter(F.col("avg_quantity") > 54)
    .orderBy(F.col("avg_quantity").desc())
)
