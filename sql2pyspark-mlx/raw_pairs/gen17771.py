from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .pivot("status", ["ACTIVE", "CANCELLED"])
    .agg(F.sum("monthly_fee").alias("total"), F.count(F.lit(1)).alias("n"))
)
