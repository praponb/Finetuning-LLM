from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(90, 190))
    .select(
        "id",
        "status",
        "start_date",
        F.round("monthly_fee", 2).alias("monthly_fee_rounded"),
    )
)
