from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("id", "status", "plan", "monthly_fee")
    .orderBy(F.col("monthly_fee").desc())
)
