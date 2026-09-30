from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") != "EXPIRED")
    .select("id", "status", "monthly_fee")
)
