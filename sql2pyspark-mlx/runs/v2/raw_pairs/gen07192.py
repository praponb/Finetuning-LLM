from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.store_id").isin([2, 4]))
    .groupBy(F.col("ss.product_id"), F.col("ss.store_id"))
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.count("*").alias("cnt"),
    )
    .orderBy("quantity_count", "cnt", "product_id", "store_id")
    .limit(5)
)
