from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "start_date",
        F.when(F.col("plan").isin(["enterprise", "premium"]), "Core").otherwise("Other").alias("plan_group"),
    )
)
