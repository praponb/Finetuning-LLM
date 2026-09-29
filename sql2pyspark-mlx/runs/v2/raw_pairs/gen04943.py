from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") < 1420)
    .select("status")
    .subtract(df_subscriptions.filter(F.col("monthly_fee") <= 130).select("status"))
    .orderBy("status")
)
