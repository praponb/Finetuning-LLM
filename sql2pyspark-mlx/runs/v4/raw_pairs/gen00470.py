from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", "store_id")
    .pivot("channel", ["mobile", "partner"])
    .agg(F.avg("shipping_fee"))
)
