from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") >= "2025-10-01")
    .select("monthly_fee")
    .subtract(df_transactions.select("amount"))
)
