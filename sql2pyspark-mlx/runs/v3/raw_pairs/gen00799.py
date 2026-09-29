from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "CANCELLED")
    .groupBy("channel")
    .agg(F.count("amount").alias("amount_count"))
    .filter(F.col("amount_count") >= 4)
)
