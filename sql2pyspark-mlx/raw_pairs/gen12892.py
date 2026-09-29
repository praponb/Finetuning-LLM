from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("status")
    .exceptAll(
        df_subscriptions
        .filter(F.col("monthly_fee") < 240)
        .select("status")
    )
)
