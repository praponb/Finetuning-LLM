from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.year("opened_date") == 2023)
    .select("status")
    .subtract(df_subscriptions.select("status"))
    .orderBy("status")
)
