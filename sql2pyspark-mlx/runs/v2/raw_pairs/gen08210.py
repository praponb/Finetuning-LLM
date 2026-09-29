from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .unionByName(df_subscriptions.filter(F.col("plan") == "premium").select("status"))
    .distinct()
    .orderBy("status")
)
