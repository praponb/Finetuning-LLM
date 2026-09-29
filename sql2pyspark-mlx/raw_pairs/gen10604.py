from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", "channel")
    .agg(F.count("*").alias("order_count"))
    .filter(F.col("order_count") < 7)
)
