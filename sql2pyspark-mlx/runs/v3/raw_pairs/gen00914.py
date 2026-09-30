from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 32)
    .select("opened_date")
    .union(df_subscriptions.select("end_date"))
)
