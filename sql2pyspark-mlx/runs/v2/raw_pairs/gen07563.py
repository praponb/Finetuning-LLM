from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(F.count("*").alias("order_count"))
    .filter(F.col("order_count") >= 5)
)
