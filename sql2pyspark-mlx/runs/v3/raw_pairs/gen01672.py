from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id") == 30)
    .groupBy("channel")
    .pivot("status", ["SHIPPED", "CANCELLED", "PENDING", "RETURNED"])
    .agg(F.avg("amount"))
    .orderBy("channel")
)
