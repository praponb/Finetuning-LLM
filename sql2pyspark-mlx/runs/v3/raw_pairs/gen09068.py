from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .groupBy(
        F.date_trunc("MONTH", F.col("ss.sale_date")).alias("sale_month"),
        F.col("ss.product_id"),
    )
    .agg(F.countDistinct(F.col("i.quantity")).alias("unique_stores"))
    .orderBy(F.col("product_id").desc())
)
