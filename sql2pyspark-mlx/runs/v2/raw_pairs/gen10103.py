from pyspark.sql import functions as F

result = (
    df_payments
    .agg(F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"))
    .orderBy(F.col("refunded_count").desc())
)
