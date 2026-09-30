from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["premium", "standard"]))
    .select("status")
    .unionByName(df_subscriptions.filter(F.col("monthly_fee") < 120).select("status"))
    .distinct()
)
