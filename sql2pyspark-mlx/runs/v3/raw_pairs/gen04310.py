from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("end_date")
    .subtract(df_accounts.select("opened_date"))
)
