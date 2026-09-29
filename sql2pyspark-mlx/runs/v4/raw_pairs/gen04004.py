from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "OPEN"]))
    .select("status")
    .intersect(df_subscriptions.select("status"))
)
