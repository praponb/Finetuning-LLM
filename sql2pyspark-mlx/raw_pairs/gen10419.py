from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.product_id").isin([3, 8]) & F.month(F.col("i.last_restock")).isin([6, 9, 10]))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("ss.units"),
        F.col("ss.product_id"),
        F.col("ss.sale_date"),
    )
)
