from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["cash", "promptpay", "bank_transfer"]))
    .groupBy("status")
    .agg(F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"))
    .orderBy(F.col("failed_count").desc(), "status")
)
