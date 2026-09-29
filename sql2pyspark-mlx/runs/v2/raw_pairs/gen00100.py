from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .groupBy(F.col("ss.product_id"), F.col("ss.store_id"))
    .agg(
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
        F.min(F.col("i.last_restock")).alias("first_last_restock"),
    )
    .orderBy(F.col("store_id").desc())
)
