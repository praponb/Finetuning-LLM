from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id", "method", "status")
    .agg(F.sum(F.when(F.col("method") == "card", 1).otherwise(0)).alias("card_count"))
)
