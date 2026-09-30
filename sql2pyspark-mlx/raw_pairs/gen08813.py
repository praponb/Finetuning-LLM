from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") >= "2024-01-01")
    .select(
        "id",
        "plan",
        "monthly_fee",
        "customer_id",
        F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled"),
        F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled_2"),
    )
    .orderBy(F.col("id").desc())
)
