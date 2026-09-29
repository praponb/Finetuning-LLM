from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "FAILED")
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
        "method",
    )
    .agg(F.max("paid_at").alias("latest_paid_at"))
    .orderBy("paid_cohort")
)
