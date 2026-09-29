from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("status").isin(["PAUSED", "ACTIVE", "EXPIRED"]) & ((F.col("plan") == "enterprise") | (F.col("status") == "CANCELLED")))
    .select("id", "status", "end_date", "customer_id")
    .orderBy("status")
)
