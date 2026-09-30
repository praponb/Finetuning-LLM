from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([1, 16, 36]) & (F.col("order_id") == 32))
    .agg(F.avg("unit_price").alias("avg_unit_price"))
)
