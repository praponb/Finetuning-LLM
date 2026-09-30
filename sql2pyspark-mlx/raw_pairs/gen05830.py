from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("id", "monthly_fee")
    .orderBy("monthly_fee", "id")
    .limit(10)
)
