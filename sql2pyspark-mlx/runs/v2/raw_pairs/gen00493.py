from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "EXPIRED")
    .groupBy(F.quarter("end_date").alias("end_quarter"))
    .pivot("plan", ["enterprise", "premium", "standard"])
    .agg(F.count("monthly_fee"))
)
