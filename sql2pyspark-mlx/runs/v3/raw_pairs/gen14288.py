from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull())
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
        F.date_trunc("MONTH", "paid_at").alias("paid_month"),
    )
    .agg(F.sum("amount").alias("total_amount"))
    .orderBy(F.col("paid_cohort").desc())
)
