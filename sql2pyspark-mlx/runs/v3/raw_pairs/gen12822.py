from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull() & F.col("status").isin(["REFUNDED", "FAILED"]))
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
    .orderBy(F.col("paid_cohort").desc())
)
