from pyspark.sql import functions as F

valid_orders = df_orders.filter(F.col("channel").isNull())

summary = (
    valid_orders
    .groupBy("channel")
    .agg(F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"))
)

result = summary
