from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 200)
    .select(
        "id",
        "start_date",
        "plan",
        "status",
        F.when(F.col("plan").isin(["enterprise", "premium"]), "Core").otherwise("Other").alias("plan_group"),
    )
)
