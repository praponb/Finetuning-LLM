from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.date_format("paid_at", "yyyy-MM").alias("paid_month"),
        "method",
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") >= 76)
)
