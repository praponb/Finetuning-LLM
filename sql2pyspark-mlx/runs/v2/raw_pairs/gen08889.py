from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "start_date",
        F.round((F.col("monthly_fee") * 1.1), 2).alias("adjusted_monthly_fee"),
    )
)
