from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") <= "2025-07-01")
    .groupBy("status")
    .pivot("plan", ["standard", "enterprise", "basic"])
    .agg(F.max("monthly_fee"))
    .orderBy("status")
)
