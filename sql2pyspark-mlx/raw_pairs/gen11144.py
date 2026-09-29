from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(70, 200))
    .select("end_date", "plan")
    .subtract(
        df_subscriptions
        .filter(F.col("customer_id").isNull())
        .select("end_date", "plan")
    )
)
