from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.product_id") == 3)
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("ss.store_id").alias("store_sale_store_id"),
        F.col("ss.product_id"),
        F.col("ss.sale_date"),
        F.date_trunc("MONTH", F.col("i.last_restock")).alias("last_restock_period"),
    )
    .orderBy("product_id")
)
