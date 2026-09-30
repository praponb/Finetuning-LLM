from pyspark.sql import functions as F

max_monthly_fee = df_subscriptions.filter(F.col("plan").isin(["enterprise", "standard"])).agg(F.max("monthly_fee")).first()[0]

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= max_monthly_fee)
    .select("id", "monthly_fee", "plan")
    .orderBy("id", F.col("monthly_fee").desc())
    .limit(3)
)
