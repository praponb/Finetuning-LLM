from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([16, 36, 40]) & (F.col("unit_price") <= 160))
    .groupBy("order_id")
    .agg(F.countDistinct("unit_price").alias("unique_orders"))
    .orderBy(F.col("unique_orders").desc())
)
