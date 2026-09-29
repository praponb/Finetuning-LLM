from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") <= "2023-08-31")
    .select(
        "id",
        "start_date",
        "monthly_fee",
        F.round("monthly_fee", 0).alias("monthly_fee_rounded"),
    )
    .orderBy(F.col("start_date").desc(), "monthly_fee")
)
