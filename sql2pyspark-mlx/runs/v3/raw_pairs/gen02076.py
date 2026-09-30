from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("status").isin(["FAILED", "SUCCESS", "REFUNDED"]))
    .groupBy("status")
    .agg(
        F.count("amount").alias("amount_count"),
        F.sum("amount").alias("total_amount"),
    )
    .orderBy("total_amount")
)
