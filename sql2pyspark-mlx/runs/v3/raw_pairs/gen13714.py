from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["enterprise", "premium", "basic"]))
    .select(
        "id",
        "start_date",
        F.last_day("start_date").alias("start_date_month_end"),
        F.lower("plan").alias("plan_lower"),
    )
)
