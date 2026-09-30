from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.store_id").isin([1, 2, 4]) & (F.col("i.quantity") >= 195))
    .select(
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("ss.product_id").alias("store_sale_product_id"),
        F.col("ss.sale_date"),
    )
    .orderBy("last_restock", "store_sale_product_id")
)
