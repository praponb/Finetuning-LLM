from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .intersect(df_subscriptions.filter(F.col("plan") == "premium").select("status"))
    .orderBy("status")
)
