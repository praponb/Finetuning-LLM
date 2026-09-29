from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 200)
    .select("plan", "status")
    .distinct()
)
