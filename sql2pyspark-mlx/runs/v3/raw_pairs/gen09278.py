from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .unionByName(df_subscriptions.filter(F.year("start_date") >= 2024).select("status"))
    .distinct()
)
