from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([2, 11, 38]))
    .select(
        "id",
        "monthly_fee",
        "start_date",
        "end_date",
        F.lower("plan").alias("plan_lower"),
    )
)
