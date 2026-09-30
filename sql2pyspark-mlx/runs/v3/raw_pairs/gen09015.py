from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull() & F.col("amount").between(1110, 1300))
    .groupBy("status", "method")
    .agg(
        F.sum(F.when(F.col("method") == "paypal", F.col("amount")).otherwise(0)).alias("paypal_amount"),
    )
    .filter(F.col("paypal_amount") > 37)
)
