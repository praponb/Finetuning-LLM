from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(210, 230) & (F.col("monthly_fee") >= 140))
    .select("id", "monthly_fee", "end_date", "status")
    .orderBy("status")
)
