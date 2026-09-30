from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .pivot("plan", ["standard", "enterprise", "premium"])
    .agg(F.avg("monthly_fee").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
)
