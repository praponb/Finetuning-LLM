from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([29, 34]) & (F.col("product_id").isin([5, 6, 15, 27]) | (F.col("order_id") == 19)))
    .withColumn("unit_price_rounded", F.round("unit_price", 0))
)
