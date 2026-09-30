from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").isNotNull())
    .select("id", "status", "end_date")
    .subtract(
        df_subscriptions
        .filter(F.col("plan").isin(["enterprise", "premium", "basic"]))
        .select("id", "status", "end_date")
    )
)
