from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select("status")
    .union(df_subscriptions.filter(F.col("plan").isNotNull()).select("plan"))
    .distinct()
)
