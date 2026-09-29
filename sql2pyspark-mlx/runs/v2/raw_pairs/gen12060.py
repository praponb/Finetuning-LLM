from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["store", "mobile"]))
    .groupBy("channel")
    .pivot("status", ["COMPLETED", "PENDING", "SHIPPED"])
    .agg(F.count("amount"))
)
