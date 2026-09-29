from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .pivot("status", ["COMPLETED", "PENDING"])
    .agg(F.max("shipping_fee"))
    .orderBy("channel")
)
