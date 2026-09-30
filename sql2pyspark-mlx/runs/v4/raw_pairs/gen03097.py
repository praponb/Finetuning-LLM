from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"))
    .filter(F.col("refunded_count") >= 4)
    .orderBy(F.col("refunded_count").desc(), "method")
    .limit(50)
)
