from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", "channel")
    .agg(F.count("amount").alias("amount_count"))
    .filter(F.col("amount_count") < 8)
    .orderBy("status")
)
