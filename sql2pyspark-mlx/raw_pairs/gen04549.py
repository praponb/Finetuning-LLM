from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 32)
    .select("end_date", "start_date")
    .intersect(
        df_subscriptions
        .filter(F.col("monthly_fee") < 70)
        .select("end_date", "start_date")
    )
)
