from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .pivot("status", ["SHIPPED", "PENDING", "CANCELLED", "RETURNED"])
    .agg(F.sum("discount"))
)
