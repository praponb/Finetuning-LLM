from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id") == 24)
    .groupBy("channel")
    .pivot("status", ["COMPLETED", "CANCELLED", "PENDING", "RETURNED"])
    .agg(F.sum("discount"))
    .orderBy("channel")
)
