from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("store_id", "customer_id")
    .pivot("channel", ["web", "store", "mobile"])
    .agg(F.sum("amount").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
