from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "monthly_fee",
        F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled"),
    )
)
