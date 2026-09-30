from pyspark.sql import functions as F

filtered_payments = df_payments.filter(F.col("amount") >= 1310)

paid_cohort_summary = (
    filtered_payments
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
        "status",
    )
    .agg(F.min("amount").alias("min_amount"))
)

result = (
    paid_cohort_summary
    .orderBy(F.col("min_amount").desc(), "paid_cohort", "status")
    .limit(5)
)
