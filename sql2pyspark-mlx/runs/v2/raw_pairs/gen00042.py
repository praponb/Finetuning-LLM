from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .groupBy(F.month(F.col("ss.sale_date")).alias("sale_month"))
    .agg(
        F.min(F.col("i.last_restock")).alias("first_last_restock"),
        F.min(F.col("i.quantity")).alias("min_quantity"),
    )
    .orderBy("first_last_restock")
)
