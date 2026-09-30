from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "start_date",
        "monthly_fee",
        F.concat("plan", F.lit(", "), "status").alias("plan_status"),
    )
)
