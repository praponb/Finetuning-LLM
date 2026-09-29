from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        "status",
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("amount")).otherwise(0)).alias("refunded_amount"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") < 1600)
    .select("status", "paid_cohort", "refunded_amount", "min_amount")
)
