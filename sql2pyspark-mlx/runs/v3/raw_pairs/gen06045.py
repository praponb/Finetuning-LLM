from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.count("*").alias("order_count"),
        F.count("*").alias("num_orders"),
        F.min("discount").alias("min_discount"),
    )
    .orderBy("channel")
)
