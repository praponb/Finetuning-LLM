from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([6, 7, 21]))
    .groupBy("channel")
    .pivot("status", ["SHIPPED", "RETURNED", "CANCELLED", "COMPLETED"])
    .agg(F.avg("amount"))
    .orderBy("channel")
)
