from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .pivot("status", ["CANCELLED", "SHIPPED", "COMPLETED"])
    .agg(F.max("discount"))
)
