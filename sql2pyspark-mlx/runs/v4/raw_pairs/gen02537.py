from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") != "PENDING")
    .groupBy("channel")
    .pivot("status", ["RETURNED", "SHIPPED", "PENDING"])
    .agg(F.count("amount"))
    .orderBy("channel")
)
