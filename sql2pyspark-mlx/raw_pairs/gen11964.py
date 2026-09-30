from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .pivot("plan", ["premium", "standard"])
    .agg(F.max("monthly_fee").alias("amount"), F.count(F.lit(1)).alias("n"))
)
