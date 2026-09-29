from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .groupBy("channel")
    .pivot("status", ["CANCELLED", "PENDING", "SHIPPED"])
    .agg(F.max("discount"))
    .orderBy("channel")
)
