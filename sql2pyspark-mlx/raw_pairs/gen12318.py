from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["paypal", "card"]))
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("method") == "promptpay", 1).otherwise(0)).alias("promptpay_count"),
    )
    .filter(F.col("promptpay_count") < 29)
)
