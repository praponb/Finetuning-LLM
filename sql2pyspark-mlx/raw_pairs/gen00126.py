from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.product_id").isin([7, 9]))
    .groupBy(F.col("ss.product_id"))
    .agg(
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
        F.min(F.col("i.quantity")).alias("min_quantity"),
    )
    .orderBy("avg_quantity")
)
