from pyspark.sql import functions as F

valid_payments = (
    df_payments
    .filter(F.col("paid_at") >= "2023-11-01")
    .withColumn("clean_status", F.trim("status"))
)

summary = (
    valid_payments
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.countDistinct("amount").alias("unique_methods"),
    )
)

result = summary.select("paid_cohort", "avg_amount", "unique_methods")
