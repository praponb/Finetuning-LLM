from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
        "status",
    )
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") < 860)
    .select("paid_cohort", "status", "min_amount")
)
