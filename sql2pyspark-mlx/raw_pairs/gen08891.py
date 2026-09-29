from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .groupBy(F.year("paid_at").alias("paid_year"))
    .agg(F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"))
)
