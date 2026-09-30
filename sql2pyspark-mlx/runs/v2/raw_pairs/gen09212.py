from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") != "SAVINGS")
    .select("status")
    .intersect(df_subscriptions.select("status"))
)
