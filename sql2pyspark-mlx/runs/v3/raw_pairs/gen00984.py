from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.store_id").isin([1, 2, 3]) & (F.col("i.product_id") == 6))
    .select(
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("ss.store_id").alias("store_sale_store_id"),
        F.add_months(F.col("i.last_restock"), 1).alias("last_restock_plus_months"),
    )
)
