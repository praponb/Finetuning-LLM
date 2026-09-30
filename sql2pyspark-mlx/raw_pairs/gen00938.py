from pyspark.sql import functions as F

max_monthly_fee = df_subscriptions.filter(F.col("status") != "ACTIVE").agg(F.max("monthly_fee")).first()[0]

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") > max_monthly_fee)
    .select("id", "monthly_fee")
    .orderBy(F.col("monthly_fee").desc(), "id")
    .limit(10)
)
