from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~(F.col("store_id").isin([1, 2, 3, 6]) | (F.col("quantity") != 166)))
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(F.min("quantity").alias("min_quantity"))
    .orderBy(F.col("min_quantity").desc(), "last_restock_month")
    .limit(10)
)
