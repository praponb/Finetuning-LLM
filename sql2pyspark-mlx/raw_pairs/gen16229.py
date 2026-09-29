from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.product_id").isin([6, 7, 9, 10]))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("ss.product_id").alias("store_sale_product_id"),
        F.col("ss.store_id").alias("store_sale_store_id"),
        F.col("ss.sale_date"),
        F.round(F.col("i.quantity"), 2).alias("quantity_rounded"),
    )
)
