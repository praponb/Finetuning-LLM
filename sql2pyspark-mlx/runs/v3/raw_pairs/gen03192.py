from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["enterprise", "basic"]) & (F.col("plan") == "premium") & (F.col("plan") != "enterprise"))
    .select(
        "id",
        "start_date",
        "plan",
        "monthly_fee",
        F.lower("plan").alias("plan_lower"),
    )
)
