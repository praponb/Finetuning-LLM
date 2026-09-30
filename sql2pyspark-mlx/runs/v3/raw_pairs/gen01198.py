from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .pivot("channel", ["partner", "web"])
    .agg(F.max("shipping_fee"))
)
