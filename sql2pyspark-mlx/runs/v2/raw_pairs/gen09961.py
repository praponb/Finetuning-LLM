from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.product_id").isin([2, 5, 10]) & (F.col("i.quantity") < 238))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("ss.revenue"),
        F.col("ss.sale_date"),
    )
    .orderBy("last_restock")
)
