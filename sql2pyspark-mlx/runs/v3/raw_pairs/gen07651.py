from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("monthly_fee")
    .orderBy(F.col("monthly_fee").desc())
)
