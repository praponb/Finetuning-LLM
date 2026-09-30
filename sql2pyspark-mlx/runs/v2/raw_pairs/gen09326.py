from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") > 220)
    .select(
        "customer_id",
        "status",
        "start_date",
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
        F.round("monthly_fee", 0).alias("monthly_fee_rounded"),
    )
    .orderBy(F.col("customer_id").desc(), "monthly_fee_rounded")
)
