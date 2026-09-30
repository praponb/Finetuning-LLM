from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "OPEN"]))
    .select("status")
    .subtract(df_subscriptions.select("status"))
)
