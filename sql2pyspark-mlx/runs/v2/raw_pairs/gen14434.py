from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.col("monthly_fee") > 200) & F.col("plan").isin(["enterprise", "standard"]))
)

status_summary = (
    base
    .groupBy("status")
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
)

result = status_summary
