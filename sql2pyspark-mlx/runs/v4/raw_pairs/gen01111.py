from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") < 1480)
    .groupBy("channel")
    .pivot("status", ["PENDING", "RETURNED", "CANCELLED", "COMPLETED"])
    .agg(F.max("shipping_fee"))
)
