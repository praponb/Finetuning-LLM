from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") < 0.33)
    .groupBy("channel")
    .pivot("status", ["CANCELLED", "PENDING"])
    .agg(F.avg("shipping_fee"))
    .orderBy("channel")
)
