from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.store_id").isin([2, 3]))
    .groupBy(F.col("ss.store_id"))
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        F.min(F.col("i.quantity")).alias("min_quantity"),
    )
    .orderBy(F.col("store_id").desc(), "min_quantity")
)
