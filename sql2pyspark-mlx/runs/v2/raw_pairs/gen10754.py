from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        F.round(((100 * F.sum(F.when(F.col("method") == "bank_transfer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bank_transfer"),
    )
    .filter(F.col("pct_bank_transfer") < 53)
    .select("status", "success_count", "pct_bank_transfer")
    .orderBy("status")
)
