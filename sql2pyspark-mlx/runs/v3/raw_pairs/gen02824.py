from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(920, 990))
    .groupBy("order_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_count"),
    )
)
