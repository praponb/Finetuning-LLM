from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["enterprise", "premium"]))
    .select(
        "id",
        "customer_id",
        "status",
        F.round((F.col("monthly_fee") * 1.1), 2).alias("adjusted_monthly_fee"),
    )
)
