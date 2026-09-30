from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~((F.col("quantity") != 85) | F.col("product_id").isin([3, 8, 10])))
    .groupBy("store_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)
