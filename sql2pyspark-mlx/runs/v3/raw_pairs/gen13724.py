from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]))
    .select("status")
    .unionByName(df_subscriptions.select("status"))
    .distinct()
)
