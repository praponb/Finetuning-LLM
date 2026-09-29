from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "monthly_fee",
        "start_date",
        "end_date",
        F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled"),
    )
    .orderBy(F.col("monthly_fee_filled").desc())
)
