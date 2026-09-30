from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "FAILED"]) & F.col("amount").between(430, 500))
)

agg = (
    base
    .groupBy("method")
    .agg(F.count("amount").alias("amount_count"))
)

result = (
    agg
    .filter(F.col("amount_count") >= 9)
    .select("method", "amount_count")
    .orderBy(F.col("amount_count").desc(), "method")
)
