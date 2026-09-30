from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & F.col("plan").isin(["premium", "standard", "enterprise"]))
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        F.ceil("monthly_fee").alias("monthly_fee_whole"),
        F.date_trunc("month", "start_date").alias("start_date_period"),
    )
)

result = (
    cleaned
    .select("id", "plan")
    .subtract(
        df_subscriptions
        .filter(F.col("monthly_fee") < 130)
        .select("id", "plan")
    )
)
