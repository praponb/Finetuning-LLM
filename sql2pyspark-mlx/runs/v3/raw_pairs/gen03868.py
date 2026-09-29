from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .pivot("status", ["CANCELLED", "RETURNED", "SHIPPED", "COMPLETED"])
    .agg(F.avg("discount"))
    .orderBy("channel")
)
