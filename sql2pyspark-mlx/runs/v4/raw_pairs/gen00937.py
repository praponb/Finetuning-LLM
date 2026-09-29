from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .pivot("status", ["RETURNED", "PENDING", "CANCELLED"])
    .agg(F.avg("discount"))
)
