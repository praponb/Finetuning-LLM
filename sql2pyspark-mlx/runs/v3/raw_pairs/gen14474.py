from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["store", "mobile"]) & F.col("status").isin(["PENDING", "CANCELLED", "SHIPPED"]))
)
