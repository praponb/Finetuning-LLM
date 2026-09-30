from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .pivot("channel", ["mobile", "store", "web"])
    .agg(F.sum("shipping_fee"))
    .orderBy("status")
)
