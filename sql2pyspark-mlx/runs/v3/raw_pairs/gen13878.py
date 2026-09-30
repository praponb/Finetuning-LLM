from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.product_id").isin([1, 7, 8]))
    .groupBy(F.col("ss.store_id"))
    .agg(
        F.max(F.col("i.quantity")).alias("max_quantity"),
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
    )
    .filter(F.col("max_quantity") >= 170)
)
