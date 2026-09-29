from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .distinct()
    .crossJoin(df_subscriptions.select("plan").distinct())
)
