from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id") == 9)
    .groupBy("status")
    .pivot("channel", ["mobile", "partner", "web"])
    .agg(F.sum("shipping_fee"))
    .orderBy("status")
)
