from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .subtract(df_subscriptions.filter(F.col("status") == "ACTIVE").select("status"))
    .orderBy(F.col("status").desc())
)
