from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2023-12-19", "2025-06-08"))
    .groupBy("status")
    .pivot("plan", ["enterprise", "premium"])
    .agg(F.count("monthly_fee"))
)
