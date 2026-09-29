from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") != "EXPIRED")
    .select("id", "start_date", "end_date")
    .subtract(
        df_subscriptions
        .filter(F.col("monthly_fee").between(80, 120))
        .select("id", "start_date", "end_date")
    )
)
