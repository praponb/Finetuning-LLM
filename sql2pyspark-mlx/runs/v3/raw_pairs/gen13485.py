from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "promptpay")
    .select("status")
    .subtract(df_subscriptions.select("status"))
)
