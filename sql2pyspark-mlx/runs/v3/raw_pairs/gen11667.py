from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([5, 6]) & (F.col("quantity") == 85))
    .groupBy("store_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)
