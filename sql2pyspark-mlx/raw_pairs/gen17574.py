from pyspark.sql import functions as F

avg_monthly_fee = df_subscriptions.agg(F.avg("monthly_fee")).first()[0]

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= avg_monthly_fee)
    .select("id", "monthly_fee", "start_date")
    .orderBy("start_date", "id")
    .limit(1)
)
