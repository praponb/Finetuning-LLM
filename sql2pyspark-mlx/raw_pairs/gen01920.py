from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") < "2024-03-01")
    .select("status")
    .unionByName(df_subscriptions.select("status"))
    .distinct()
)
