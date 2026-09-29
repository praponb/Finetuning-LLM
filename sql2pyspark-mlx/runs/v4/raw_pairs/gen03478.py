from pyspark.sql import functions as F

valid_payments = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (F.col("paid_at") < "2023-09-01") & F.col("order_id").isNotNull())
)

agg = (
    valid_payments
    .groupBy(
        "order_id",
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
    )
)

result = (
    agg
    .filter(F.col("pct_refunded") > 45)
    .select("order_id", "paid_cohort")
    .orderBy("paid_cohort")
)
