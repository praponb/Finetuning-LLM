from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.product_id") == 4)
    .groupBy(F.col("ss.product_id"))
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
    )
    .orderBy("latest_last_restock", "product_id")
    .limit(5)
)
