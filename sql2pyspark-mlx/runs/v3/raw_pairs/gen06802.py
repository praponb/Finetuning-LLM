from pyspark.sql import functions as F

avg_monthly_fee = df_subscriptions.filter(~F.col("plan").isin(["premium", "standard"])).agg(F.avg("monthly_fee")).first()[0]

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") > avg_monthly_fee)
    .select("id", "monthly_fee")
)
