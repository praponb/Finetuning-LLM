from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "premium")
    .select("plan", "start_date")
    .intersect(
        df_subscriptions
        .filter(F.col("monthly_fee") >= 150)
        .select("plan", "start_date")
    )
    .orderBy("start_date")
)
