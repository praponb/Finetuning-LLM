from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["standard", "enterprise"]) & F.col("plan").isin(["basic", "enterprise"]))
    .select(
        "end_date",
        "monthly_fee",
        "status",
        F.length("status").alias("status_length"),
    )
    .orderBy("status")
)
