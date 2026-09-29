from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .groupBy(F.col("ss.product_id"))
    .agg(
        F.countDistinct(F.col("i.quantity")).alias("unique_stores"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
    )
    .orderBy(F.col("unique_stores").desc(), "product_id")
    .limit(10)
)
