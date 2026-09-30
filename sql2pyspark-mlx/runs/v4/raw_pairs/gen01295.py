from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("status").isin(["SUCCESS", "REFUNDED"]) & ((F.col("amount") >= 1470) | (F.col("status") != "SUCCESS")))
    .groupBy("method")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "promptpay", 1).otherwise(0))) / F.count("*")), 1).alias("pct_promptpay"),
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount")).otherwise(0)).alias("success_amount"),
    )
    .orderBy("success_amount")
)
