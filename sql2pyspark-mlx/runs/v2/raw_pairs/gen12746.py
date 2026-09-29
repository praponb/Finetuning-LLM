from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", "customer_id")
    .agg(F.count("amount").alias("amount_count"))
    .filter(F.col("amount_count") < 7)
)
