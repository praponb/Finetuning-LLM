from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([4, 7, 8, 11]))
    .select("unit_price", F.floor("unit_price").alias("unit_price_whole"))
)
