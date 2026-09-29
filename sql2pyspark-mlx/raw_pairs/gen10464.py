from pyspark.sql import functions as F

min_monthly_fee = df_subscriptions.agg(F.min("monthly_fee")).first()[0]

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= min_monthly_fee)
    .select("id", "monthly_fee", "plan")
)
