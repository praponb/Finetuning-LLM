from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([20, 21]) & F.col("amount").between(600, 1340))
    .groupBy("status", "order_id", "method")
    .agg(F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"))
    .filter(F.col("refunded_count") >= 4)
)
