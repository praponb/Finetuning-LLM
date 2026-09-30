from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") >= "2023-09-01")
    .select("status")
    .unionByName(df_subscriptions.filter(F.col("status") != "EXPIRED").select("status"))
    .orderBy("status")
)
