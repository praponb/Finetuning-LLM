from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "SAVINGS")
    .select("status")
    .unionByName(df_subscriptions.filter(F.year("end_date") >= 2023).select("status"))
    .distinct()
    .orderBy("status")
)
