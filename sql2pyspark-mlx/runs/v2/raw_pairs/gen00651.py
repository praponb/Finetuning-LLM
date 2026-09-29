from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "FROZEN", "CLOSED"]))
    .select("status")
    .subtract(df_subscriptions.select("status"))
)
