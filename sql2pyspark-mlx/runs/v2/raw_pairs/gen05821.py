from pyspark.sql import functions as F

src = df_subscriptions.filter(F.col("status") == "PAUSED")

summary = (
    src
    .groupBy("plan")
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
)

result = summary
