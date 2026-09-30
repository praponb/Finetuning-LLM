from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["paypal", "promptpay"]))
    .groupBy("status", "method")
    .agg(
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
    )
    .filter(F.col("failed_count") > 46)
    .select("status", "method", "failed_count")
)
