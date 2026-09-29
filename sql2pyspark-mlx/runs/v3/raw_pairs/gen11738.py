from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2024-05-01")
    .groupBy(
        "method",
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_count"),
        F.count("amount").alias("amount_count"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
    )
    .filter(F.col("cash_count") >= 5)
)
