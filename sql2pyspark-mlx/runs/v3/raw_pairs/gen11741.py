from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .groupBy(F.month(F.col("ss.sale_date")).alias("sale_month"))
    .agg(
        F.max(F.col("i.quantity")).alias("max_quantity"),
        F.countDistinct(F.col("i.quantity")).alias("unique_products"),
    )
    .orderBy("sale_month")
)
