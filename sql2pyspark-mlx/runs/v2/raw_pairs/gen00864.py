from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", "status")
    .agg(
        F.sum(F.when(F.col("method") == "paypal", F.col("amount")).otherwise(0)).alias("paypal_amount"),
    )
    .filter(F.col("paypal_amount") < 77)
    .select("method", "status", "paypal_amount")
    .orderBy(F.col("paypal_amount").desc(), F.col("status").desc(), "method")
    .limit(10)
)
