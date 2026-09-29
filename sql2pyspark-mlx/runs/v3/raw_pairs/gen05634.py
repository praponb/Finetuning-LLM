from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("monthly_fee")
    .subtract(df_transactions.select("amount"))
    .orderBy("monthly_fee")
)
