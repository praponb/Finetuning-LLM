from pyspark.sql import functions as F

result = (
    df_payments
    .select("amount")
    .union(df_subscriptions.select("monthly_fee"))
    .distinct()
    .orderBy("amount")
)
