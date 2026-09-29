from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "FAILED"]) & (F.col("method") == "promptpay"))
    .groupBy("method")
    .agg(F.count("amount").alias("amount_count"))
    .filter(F.col("amount_count") < 5)
    .orderBy(F.col("amount_count").desc(), "method")
    .limit(10)
)
