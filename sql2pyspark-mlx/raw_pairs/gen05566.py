from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"))
    .pivot("status", ["SUCCESS", "FAILED", "REFUNDED"])
    .agg(F.avg("amount"))
)
