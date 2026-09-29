from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("plan").isin(["standard", "premium"]))
    .select(
        "id",
        "plan",
        "end_date",
        "customer_id",
        F.length("plan").alias("plan_length"),
    )
)
