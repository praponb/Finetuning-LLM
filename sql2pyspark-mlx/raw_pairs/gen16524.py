from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "monthly_fee",
        "plan",
        (F.datediff(F.current_date(), "start_date") / 365.25).alias("years_since_start_date"),
    )
    .orderBy("plan")
)
