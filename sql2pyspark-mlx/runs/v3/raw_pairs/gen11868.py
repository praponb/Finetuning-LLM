from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .pivot("status", ["CANCELLED", "PENDING", "SHIPPED", "COMPLETED"])
    .agg(F.count("discount"))
    .orderBy("channel")
)
