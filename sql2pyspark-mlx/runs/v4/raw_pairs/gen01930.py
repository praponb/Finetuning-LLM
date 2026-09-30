from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "premium")
    .select(
        "id",
        "status",
        "plan",
        "customer_id",
        F.ceil("monthly_fee").alias("monthly_fee_whole"),
    )
)
