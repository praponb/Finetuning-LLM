from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 80)
    .select("id", "start_date")
    .intersect(
        df_subscriptions
        .filter(F.col("plan").isin(["enterprise", "basic"]))
        .select("id", "start_date")
    )
)
