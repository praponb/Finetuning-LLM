from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([13, 38]) | F.col("status").isin(["REFUNDED", "FAILED", "SUCCESS"]))
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.min("paid_at").alias("first_paid_at"),
    )
    .filter(F.col("avg_amount") < 57)
    .orderBy(F.col("avg_amount").desc())
)
