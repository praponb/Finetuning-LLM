from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "monthly_fee",
        "status",
        F.date_trunc("month", "end_date").alias("end_date_period"),
        F.round((F.col("monthly_fee") * 1.15), 2).alias("monthly_fee_adjusted"),
    )
    .orderBy("status")
)
