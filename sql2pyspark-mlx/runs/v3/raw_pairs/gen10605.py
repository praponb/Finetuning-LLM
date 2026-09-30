from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .intersect(df_subscriptions.select("status"))
    .orderBy(F.col("status").desc())
)
