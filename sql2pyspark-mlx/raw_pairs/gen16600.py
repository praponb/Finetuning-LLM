from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("customer_id")
    .subtract(
        df_subscriptions
        .filter(F.col("monthly_fee").isNotNull())
        .select("customer_id")
    )
)
