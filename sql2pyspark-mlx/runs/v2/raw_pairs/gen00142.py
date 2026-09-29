from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .groupBy(F.date_trunc("MONTH", F.col("ss.sale_date")).alias("sale_month"))
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
    )
    .orderBy("sale_month", F.col("stddev_quantity").desc())
)
